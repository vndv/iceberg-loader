import contextlib
import json
import logging
import os
import sys
import traceback
from collections.abc import Callable, Iterator, Mapping
from logging import Logger, LogRecord
from typing import Any, Protocol

_LOGGER_NAME = 'iceberg_loader'

# Like any library, emit nothing by default: records propagate to the application's handlers.
logging.getLogger(_LOGGER_NAME).addHandler(logging.NullHandler())

# Set only by configure_logging()
LOGGER: Logger | None = None
logger = sys.modules[__name__]  # expose module as logger, for backward compatibility

_MESSAGE_LOGGING_METHODS = {
    'debug',
    'info',
    'warning',
    'warn',
    'error',
    'critical',
    'exception',
    'fatal',
}


class LogMethod(Protocol):
    def __call__(self, msg: Any, *args: Any, **kwargs: Any) -> Any: ...


def __getattr__(name: str) -> LogMethod | Any:
    """
    Forwards log method calls (debug, info, error etc.) to the global LOGGER instance.
    Allows usage: from iceberg_loader import logger; logger.info("message")
    """

    def wrapper(msg: Any, *args: Any, **kwargs: Any) -> Any:
        target: Callable[..., Any] = getattr(get_logger(), name)

        if name in _MESSAGE_LOGGING_METHODS:
            kwargs.setdefault('stacklevel', 2)
            if name == 'exception':
                kwargs.setdefault('stacklevel', 3)

        return target(msg, *args, **kwargs)

    if name in _MESSAGE_LOGGING_METHODS:
        wrapper.__name__ = name
        return wrapper

    return getattr(get_logger(), name)

    raise AttributeError(f'module {__name__} has no attribute {name}')


def metrics(name: str, extra: Mapping[str, Any], stacklevel: int = 1) -> None:
    get_logger().debug(f'METRIC: {name}', extra={'metrics': extra}, stacklevel=stacklevel)


@contextlib.contextmanager
def suppress_and_warn(msg: str) -> Iterator[None]:
    """Context manager to catch exceptions, log them as warnings, and suppress them."""
    try:
        yield
    except Exception:
        get_logger().warning(msg, exc_info=True)


def is_logging() -> bool:
    return LOGGER is not None


def log_level() -> str:
    if not LOGGER:
        return 'NOTSET'
    return logging.getLevelName(LOGGER.level)


def pretty_format_exception() -> str:
    return traceback.format_exc()


_LEVEL_COLORS = {
    logging.DEBUG: '\x1b[2m',
    logging.INFO: '\x1b[32m',
    logging.WARNING: '\x1b[33m',
    logging.ERROR: '\x1b[31m',
    logging.CRITICAL: '\x1b[1;31m',
}
_RESET = '\x1b[0m'


class TextFormatter(logging.Formatter):
    """Formatter for human-readable text logs; use_color wraps the level name in ANSI colors."""

    def __init__(self, *args: Any, use_color: bool = False, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.use_color = use_color

    def format(self, record: LogRecord) -> str:
        color = _LEVEL_COLORS.get(record.levelno) if self.use_color else None
        if color:
            # Other handlers share the record, so color a copy.
            record = logging.makeLogRecord(record.__dict__)
            record.levelname = f'{color}{record.levelname}{_RESET}'
        s = super().format(record)
        metrics_data = getattr(record, 'metrics', None)
        if metrics_data:
            s = f'{s} | metrics={json.dumps(metrics_data)}'
        return s


class JsonFormatter(logging.Formatter):
    """Formatter for JSON logs."""

    def __init__(self, component: str = 'iceberg-loader', version: Mapping[str, str] | None = None, **kwargs: Any):
        super().__init__(**kwargs)
        self.component = component
        self.version = version

    def format(self, record: LogRecord) -> str:
        log_obj: dict[str, Any] = {
            'time': self.formatTime(record, self.datefmt),
            'level': record.levelname,
            'message': record.getMessage(),
            'logger': record.name,
            'component': self.component,
            'module': record.module,
            'line': record.lineno,
            'process': record.process,
            'thread': record.threadName,
        }
        if record.exc_info:
            log_obj['exception'] = self.formatException(record.exc_info)
        if hasattr(record, 'metrics'):
            log_obj['metrics'] = record.metrics
        if self.version:
            log_obj['version'] = self.version
        return json.dumps(log_obj)


def configure_logging(
    level: str = 'INFO',
    log_format: str = 'TEXT',  # "TEXT" or "JSON"
    component: str = 'iceberg-loader',
    version: Mapping[str, str] | None = None,
) -> Logger:
    """Attaches a stderr handler to the iceberg_loader logger and stops propagation to the root logger."""
    # LOGGER marks that configure_logging() ran (is_logging, log_level, get_logger) and tests reset it.
    global LOGGER  # noqa: PLW0603

    logger = logging.getLogger(_LOGGER_NAME)
    logger.setLevel(level.upper())

    if logger.hasHandlers():
        logger.handlers.clear()

    handler = logging.StreamHandler(sys.stderr)

    formatter: logging.Formatter
    if log_format.upper() == 'JSON':
        formatter = JsonFormatter(component=component, version=version)
    else:
        formatter = TextFormatter(
            fmt='{asctime} [{levelname}] {name}: {message}', style='{', use_color=_stderr_supports_color()
        )

    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False

    LOGGER = logger
    return logger


def _stderr_supports_color() -> bool:
    # https://no-color.org: any non-empty NO_COLOR disables color. stderr is None under pythonw.
    return sys.stderr is not None and sys.stderr.isatty() and not os.environ.get('NO_COLOR')


def get_logger() -> Logger:
    """Returns the iceberg_loader logger; it has no handlers of its own unless configure_logging() was called."""
    return LOGGER or logging.getLogger(_LOGGER_NAME)
