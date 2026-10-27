# Как сделать iceberg-loader blazing fast

Исследование октябрь 2026: внешний анализ PyIceberg 0.10–0.11 (исходники, issues/PR, бенчмарки) + разбор кода iceberg-loader v0.3.0.

## TL;DR

Горячий путь — **не Parquet-writer**. Узкое место — **количество коммитов** и **rewrite-пути** (`overwrite`/`delete`). Параллелизм записи в PyIceberg уже встроен, но не включается, потому что на коммит получается 1 файл.

Порядок рычагов по эффекту: **коммиты → параллельные файлы → rewrite-пути → микро-оптимизации conform**.

## 1. Коммиты — главный рычаг

- `commit_interval=0` (дефолт) в коде → `max(1, commit_interval)` в `loader.py:109` → **1 батч = 1 коммит**.
- Каждый коммит = новый `metadata.json` (полная копия метаданных), manifest-list, манифест, RTT каталога.
- `load_data(pa.Table)` сначала делает `table_data.to_batches()` (`loader.py:51`): один большой `pa.Table` может развалиться на N чанков = N коммитов.
- На мелких загрузках коммит доминирует. Бенчмарк `#3129` для иллюстрации состава стоимости: upsert 1M строк — 17.5 мин, из них append+commit всего 1.14 s, остальное — delete-скан.

Что делать:
- Поднять дефолт/задокументировать `commit_interval` 32–128 для стриминга. Цена — атомарность: коммит атомарен только в пределах буфера (`AGENTS.md`, traps).
- В `load_data` при `commit_interval=0` подавать таблицу одним батчем, а не через `to_batches()`.
- Для профиля «частые мелкие коммиты» — `commit.manifest-merge.enabled=true` (дефолтный fast append множит манифесты; merge-append сливает до 8 MB / min-count 100: дороже коммит, дешевле чтение).

## 2. Параллелизм записи — уже есть, но не используется

- PyIceberg пишет файлы через `ThreadPoolExecutor` (`ExecutorFactory`, `PYICEBERG_MAX_WORKERS`), один `WriteTask` = один файл.
- Бенчмарк apache/iceberg-python #428 / PR #444: 1 файл (2 GB) — 31 s; 14 файлов параллельно — **3.9 s (8×)** на c5ad.8xlarge (~6.7 GB Arrow). Потолок одиночного потока ≈ 60–70 MB/s Arrow, параллельно ≈ 1.7 GB/s.
- `write.target-file-size-bytes` по умолчанию 512 MB и считается в **несжатых Arrow-байтах** (`Table.nbytes`); на диске после zstd файл в 3–10× меньше. Один буфер на 300 MB → 1 файл → 1 поток → нулевой выигрыш.
- `pyarrow` file IO не async; другого встроенного параллелизма нет.

Что делать:
- Экспонировать `write.target-file-size-bytes` (ориентир 256–512 MB uncompressed → файлы ~50–100 MB на диске) и задокументировать связку «файлов на коммит ≈ 2–4 × workers», `PYICEBERG_MAX_WORKERS = cpu/2`.

## 3. Rewrite-пути в `src/iceberg_loader/core/strategies.py`

- `OverwriteStrategy` на первой записи делает `txn.overwrite(data)` — **полная замена таблицы**. Это дефолтный `write_mode` — дефолт опасен и дорог.
- `IdempotentStrategy` делает `txn.delete(replace_filter)`: PyIceberg при «слабом» фильтре **перечитывает и перезаписывает целые файлы**. Если `replace_filter` выровнен по партициям (identity-трансформы), есть `table.transaction().dynamic_partition_overwrite(df)` — удаление по метаданным партиций без чтения данных.
- `UpsertStrategy` — `table.upsert()` на каждый буфер:
  - PyIceberg 0.10+: coarse match filter + anti-join (PR #2943): upsert 2M строк **11.9 → 2.3 мин (5.1×)**, на больших прогонах до **9.3×**; создание фильтра 7.9s → 0.07s, скан 382s → 1.4s.
  - Замер #3129 (0.11.0, 1M строк): `txn.delete()` 1054 s против append+commit 1.14 s — upsert быстр, только пока delete-скан дешёвый.
  - PR #1685: дедупликация через pyarrow-join — 2.04 s против 3.52 s (1.7×).

Что делать:
- `dynamic_partition_overwrite` при identity-партиционировании + `replace_filter`, выровненном по партициям.
- Предупреждать в result dict о rewrite-путях.
- Не строить match-фильтры на стороне лоадера.

## 4. Версия PyIceberg

Пин сейчас — `pyiceberg>=0.10.0`. **0.11.0** (2026) закрывает бесплатно:

- generator-based writes: пиковая память после записи 151.6 MB → ~30.5 MB на 128 MB записи (PR #2671);
- фикс O(N²) роста кэша манифестов (PR #2951);
- commit retry с **переиспользованием записанных data-файлов** и перегенерацией только манифестов: быстрее user-side retry в 1.3–1.8× (PR #3320), для Glue оптимален `commit.retry.min-wait-ms=500`;
- multi-process-safe `ExecutorFactory` (PR #2546);
- `DeleteFileIndex` (PR #2918); `maintenance.*` API (expire_snapshots, rewrite_data_files).

Минимум для «blazing fast» — `pyiceberg>=0.11`.

Про Rust (`pyiceberg-core` / iceberg-rust 0.10): в write-путь PyIceberg он **не встроен** — только partition-трансформы и DataFusion-чтение. Ставить ради скорости записи бессмысленно (extras уже тянут его ради трансформов).

## 5. Микро-оптимизации в коде loader

| Место | Проблема | Правка |
| --- | --- | --- |
| `conform.py:52` | `pa.Table.from_batches([b])` на каждый батч, потом concat | `pa.Table.from_batches(buffer)` один раз |
| `conform.py:55 + 71` | **двойной** `pa.concat_tables` — два полных копирования буфера | кастовать батчи и конкатенировать один раз |
| `conform.py:70` | `get_arrow_schema(table)` (Iceberg→Arrow) пересобирается каждый буфер | кэш на время загрузки |
| `conform.py:76` | `pa.array([ts]*len(data))` на каждый батч | один раз на загрузку / после concat |
| `conform.py:93-98` | `_distinct_schemas` — O(n²) сравнений схем | дедуп по хэшу схемы |
| `schema.py:69-95` | `update_schema()` транзакция на каждую новую схему | собрать все новые колонки → одна транзакция |
| `utils/arrow.py:92-101` | каст `safe=True`, при ошибке повтор `safe=False` — исключения на каждую плохую колонку каждый буфер | решить путь каста по колонке один раз за загрузку |
| `register.py` | футеры Parquet читаются последовательно | распараллелить через `ExecutorFactory` |

Maintenance: `expire_snapshots` уже есть (`services/maintenance.py`); добавить профиль «`expire_snapshots` + `maintenance.rewrite_data_files()` после больших серий мелких загрузок». `write.metadata.previous-versions-max` по умолчанию 100 — ок.

## 6. Чего делать НЕ надо

- **Кодек не трогать**: zstd-3 — оптимум (на 7% медленнее snappy, файл на 33% меньше: 68.61 MiB/0.94s против 102.17 MiB/0.88s на 10M строк). `compression-level=9` — 2.5× медленнее, выигрыш <0.15%. Словарное кодирование не отключать (+28% размера без него).
- **Row group не уменьшать**: дефолт 1M строк сжимает лучше; уменьшать только для очень широких таблиц ради памяти.
- **`write.distribution-mode`**: в PyIceberg не реализован, игнорируется (Java/Spark-концепт).
- **Стриминговый `append(RecordBatchReader)`**: constant memory (peak RSS +58 MiB на 515 MiB, PR #3336), но только unpartitioned и последовательный — опция для больших unpartitioned загрузок, не общий путь (теряется conform).

## 7. Two-stage write/commit (горизонт масштабирования за пределы одного процесса)

Официально поддержанный паттерн apache/iceberg-python #1751:

```python
# Процесс A: писать файлы без коммита (параллельно, сколько угодно воркеров)
data_files = list(
    pyiceberg.io.pyarrow._dataframe_to_data_files(
        table_metadata=table.metadata, write_uuid=uuid.uuid4(), df=pa_df, io=table.io
    )
)

# Процесс B: централизованный коммит
with table.transaction() as trx:
    with trx.update_snapshot().fast_append() as update_snapshot:
        for data_file in data_files:
            update_snapshot.append_data_file(data_file)
```

## 8. Реалистичные потолки и бенчмарки

| Источник | Что мерил | Результат |
| --- | --- | --- |
| #428 / PR #444 | 6.7 GB Arrow → S3, c5ad.8xlarge | 1 файл: 31s; 14 параллельно: 3.9s (~1.7 GB/s Arrow) |
| PR #2943 | upsert 2M строк | 5.1–9.3× после coarse-filter |
| #3129 | upsert 1M строк, 0.11.0 | 17.5 мин, delete 1054s, append+commit 1.14s |
| PR #3320 | конкурентные аппенды, Glue+S3 | internal retry 1.3–1.8× |
| Bodo blog (2025-07) | TPC-H SF1000, pyiceberg 0.9.1 | timeout >12h на 1 ноде (большие объёмы — не профиль PyIceberg) |
| IcedHops (KTH 2025) | PyIceberg vs Spark | write latency в 40–140× ниже Spark на малых таблицах |

Потолок single-process PyIceberg: ~1.5–2 GB/s несжатого Arrow при 8+ параллельных файлах. Для типичного профиля iceberg-loader (микробатчи, сотни MB) узкое место — число коммитов и rewrite-пути, а не writer.

## 9. План внедрения

- **Фаза 1** (дешёво, без изменения семантики): кэш схемы и timestamp-массива, схлопнуть двойной concat, дедуп схем за O(n), батч-эволюция схемы одной транзакцией, путь каста по колонке один раз за загрузку.
- **Фаза 2** (рычаги, меняют поведение/зависимости): дефолтный `commit_interval` + `load_data` одним батчем, экспонировать `write.target-file-size-bytes` и `commit.manifest-merge.enabled`, `PYICEBERG_MAX_WORKERS` в docs, `pyiceberg>=0.11`, `dynamic_partition_overwrite` для партиционированного replace.
- **Фаза 3** (измеримо): benchmark-харнесс на sqlite-каталоге — каждая правка с цифрой до/после.

Открытые вопросы: готовность поднять `pyiceberg>=0.11` (breaking для зависимостей) и поменять дефолты `commit_interval`/`load_data` (меняет атомарность).

## 10. Фаза 1: результаты (2026-10-27)

Бенчмарк `local/blazing-bench/bench.py`: sqlite-каталог, `load_batches_to_iceberg`, 200 батчей × 10k строк (8 колонок), `commit_interval=50`, `write_mode='append'`, `load_timestamp` задан. Сценарии: (а) одна схема; (б) `schema_evolution=True`, каждый 10-й батч добавляет колонку (`extra_1..extra_19`, по 5 новых схем на буфер); (в) две колонки float64 с дробями в int64-таблицу (`safe=False`). Медиана 5–11 прогонов после warm-up, pyiceberg 0.12.0, pyarrow 25.0.1, Apple Silicon. «До» — состояние после предыдущей принятой правки. Решение по 5% принималось по `conform_ms` в целевом сценарии.

| Правка | Сценарий | conform_ms до → после | wall_ms до → после | Итог |
| --- | --- | --- | --- | --- |
| `_load_dttm`: `pa.repeat` от закэшированного scalar вместо `pa.array([ts] * n)` | а / б / в | 101.4 → 5.8 / 192.4 → 91.8 / 101.9 → 5.7 | 406.8 → 306.4 / 626.4 → 526.3 / 368.8 → 273.6 | оставлена |
| Один `pa.Table.from_batches(buffer)` вместо per-batch + `concat_tables` (путь без evolution) | а (A/B, 11 прогонов) | 6.0 → 5.5 | 313 → 311 (в шуме) | оставлена (−8% conform, ~0.5 мс абсолютно) |
| Кэш `get_arrow_schema` по `current_schema_id` | а / б | 5.5 → 5.5 / 93.9 → 97.9 | — | **откатана**, не дала эффекта |
| `_distinct_schemas` за O(n) | б (A/B) | 95.3 → 99.4 | 543 → 548 | **откатана**, не дала эффекта |
| Все новые колонки буфера (включая `_load_dttm`) одной транзакцией `update_schema()` | б | 95.3 → 22.8 | 543 → 449 | оставлена |
| Путь каста safe/unsafe запоминается для колонки на всю загрузку | в (A/B) | 5.2 → 5.2 | 276 → 275 | **откатана**, не дала эффекта |

Итог baseline → финал: (а) conform 101.4 → 5.5 мс, wall 407 → 307 мс (−25%); (б) conform 192.4 → 22.3 мс, wall 626 → 440 мс (−30%); (в) conform 101.9 → 5.3 мс, wall 369 → 278 мс (−25%). Теперь conform — 2–5% wall, остальное — запись Parquet и коммит.

Заметки:
- 95% исходного conform — Python-список из 2M `datetime` для `_load_dttm`. Колонка по-прежнему добавляется к каждой таблице до создания таблицы: так `day(_load_dttm)` партиционирование и порядок колонок не меняются, а «один раз после concat» в evolution-пути дал бы дубль колонки после NULL-заполнения в `convert_table_types`. Значения совпадают с прежними для naive и tz-aware `datetime`.
- `pa.concat_tables` без `promote_options` не копирует данные (склеивает chunks), поэтому «двойное копирование» из §5 не подтвердилось — выигрыш правки только в overhead на 200 мелких таблиц.
- Кэш схемы, дедуп и кэш каста экономят микросекунды: Iceberg→Arrow конверсия, сравнение ≤6 схем и неудачный safe-каст на 500k значений стоят доли миллисекунды на буфер.
- Одна транзакция: порядок колонок прежний (`_load_dttm`, затем новые колонки в порядке первого появления, при совпадении имени выигрывает первый тип); INFO-лог «Evolving schema» теперь одна строка на буфер; у таблицы становится меньше версий схемы (тест `test_new_columns_from_one_buffer_are_added_in_one_schema_commit`: 2 схемы вместо 5). Если у одной из схем буфера тип не конвертируется в Iceberg, теперь не коммитится ничего; раньше колонки из предыдущих схем успевали добавиться. Внутренний `SchemaManager.evolve_schema_if_needed` принимает `(table, *batch_schemas)`, вызовы с одной схемой совместимы.
- `from_batches` на батчах, отличающихся только metadata схемы или полей, работает как прежний `concat_tables`; на разных полях так же бросает `ArrowInvalid`.

## Источники

- PyIceberg source (main, ~0.11): `pyiceberg/io/pyarrow.py`, `pyiceberg/table/__init__.py`, `pyiceberg/table/update/snapshot.py`, `pyiceberg/utils/concurrent.py`
- GitHub: #428, PR #444, #1751, PR #2671, PR #3336, PR #3320, PR #2943, #2159, #3129, PR #1685, #2138, #2152
- Релизы/блоги: PyIceberg 0.11.0 release blog, Iceberg Rust 0.10, Bodo benchmark (2025-07), IcedHops (KTH 2025), Parquet codecs benchmark (2026), astronomy-commons/lsdb#695
- Локально: `src/iceberg_loader/core/conform.py`, `src/iceberg_loader/core/loader.py`, `src/iceberg_loader/core/strategies.py`, `src/iceberg_loader/core/schema.py`, `src/iceberg_loader/utils/arrow.py`, `local/compaction-spike/spike.py`
