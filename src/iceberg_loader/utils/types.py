import pyarrow as pa
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IcebergType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
    TimestamptzType,
)


class TypeRegistry:
    def __init__(self) -> None:
        self._arrow_to_iceberg = self._build_arrow_to_iceberg()
        self._iceberg_to_arrow = self._build_iceberg_to_arrow()
        self._custom_mappings: dict[pa.DataType, IcebergType] = {}

    def _build_arrow_to_iceberg(self) -> dict[pa.DataType, IcebergType]:
        return {
            pa.string(): StringType(),
            pa.int32(): IntegerType(),
            pa.int64(): LongType(),
            pa.float32(): FloatType(),
            pa.float64(): DoubleType(),
            pa.bool_(): BooleanType(),
            pa.binary(): BinaryType(),
            pa.date32(): DateType(),
            pa.timestamp('ns'): TimestampType(),
            pa.timestamp('ns', tz='UTC'): TimestamptzType(),
            pa.timestamp('us'): TimestampType(),
            pa.timestamp('ms'): TimestampType(),
            pa.timestamp('s'): TimestampType(),
            pa.null(): StringType(),
        }

    def _build_iceberg_to_arrow(self) -> dict[type[IcebergType], pa.DataType]:
        return {
            StringType: pa.string(),
            IntegerType: pa.int32(),
            LongType: pa.int64(),
            FloatType: pa.float32(),
            DoubleType: pa.float64(),
            BooleanType: pa.bool_(),
            BinaryType: pa.binary(),
            DateType: pa.date32(),
            TimestampType: pa.timestamp('us'),
            TimestamptzType: pa.timestamp('us', tz='UTC'),
        }

    def register_custom_mapping(self, arrow_type: pa.DataType, iceberg_type: IcebergType) -> None:
        self._custom_mappings[arrow_type] = iceberg_type

    def get_iceberg_type(self, arrow_type: pa.DataType) -> IcebergType:
        if pa.types.is_null(arrow_type):
            return StringType()
        if arrow_type in self._custom_mappings:
            return self._custom_mappings[arrow_type]

        if arrow_type in self._arrow_to_iceberg:
            return self._arrow_to_iceberg[arrow_type]

        if pa.types.is_timestamp(arrow_type):
            if arrow_type.tz is not None:
                return TimestamptzType()
            return TimestampType()

        if pa.types.is_decimal(arrow_type):
            precision = getattr(arrow_type, 'precision', 38)
            scale = getattr(arrow_type, 'scale', 0)
            return DecimalType(precision=precision, scale=scale)

        raise ValueError(f'Unsupported PyArrow type: {arrow_type}')

    def get_arrow_type(self, iceberg_type_instance: IcebergType) -> pa.DataType:
        if isinstance(iceberg_type_instance, DecimalType):
            return pa.decimal128(iceberg_type_instance.precision, iceberg_type_instance.scale)

        type_class = type(iceberg_type_instance)

        if type_class in self._iceberg_to_arrow:
            return self._iceberg_to_arrow[type_class]

        raise ValueError(f'Unsupported Iceberg type: {iceberg_type_instance} (type: {type_class})')

    def validate_mapping(self, arrow_type: pa.DataType, iceberg_type: IcebergType) -> bool:
        try:
            self.get_iceberg_type(arrow_type)
            self.get_arrow_type(iceberg_type)
            return True
        except ValueError:
            return False


_type_registry = TypeRegistry()


def get_iceberg_type(arrow_type: pa.DataType) -> IcebergType:
    return _type_registry.get_iceberg_type(arrow_type)


def get_arrow_type(iceberg_type_instance: IcebergType) -> pa.DataType:
    return _type_registry.get_arrow_type(iceberg_type_instance)


def register_custom_mapping(arrow_type: pa.DataType, iceberg_type: IcebergType) -> None:
    _type_registry.register_custom_mapping(arrow_type, iceberg_type)
