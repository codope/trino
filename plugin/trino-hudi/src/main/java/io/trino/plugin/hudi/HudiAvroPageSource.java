package io.trino.plugin.hudi;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignatureParameter;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.hadoop.realtime.HoodieMergeOnReadSnapshotReader;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hudi.HudiTypeUtil.toTrinoTimestamp;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.Decimals.encodeShortScaledValue;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.LongTimestampWithTimeZone.fromEpochMillisAndFraction;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
import static java.lang.Math.floorDiv;
import static java.lang.Math.floorMod;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

public class HudiAvroPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(HudiAvroPageSource.class);
    private static final AvroDecimalConverter DECIMAL_CONVERTER = new AvroDecimalConverter();

    private final HudiSplit hudiSplit;
    private final Schema schema;
    private final Iterator<HoodieRecord> records;
    private final HoodieMergeOnReadSnapshotReader snapshotReader;
    private final PageBuilder pageBuilder;
    private final List<String> columnNames;
    private final List<Type> columnTypes;
    private final AtomicLong readBytes;

    public HudiAvroPageSource(
            HudiSplit hudiSplit,
            Schema schema,
            Iterator<HoodieRecord> records,
            HoodieMergeOnReadSnapshotReader snapshotReader,
            List<String> columnNames,
            List<Type> columnTypes)
    {
        this.hudiSplit = hudiSplit;
        this.schema = schema;
        this.records = records;
        this.snapshotReader = snapshotReader;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.pageBuilder = new PageBuilder(columnTypes);
        this.readBytes = new AtomicLong();
    }

    @Override
    public long getCompletedBytes()
    {
        return readBytes.get();
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return !records.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        checkState(pageBuilder.isEmpty(), "PageBuilder is not empty at the beginning of a new page");
        if (!records.hasNext()) {
            return null;
        }
        while (records.hasNext()) {
            HoodieRecord<HoodieAvroIndexedRecord> record = records.next();
            pageBuilder.declarePosition();
            for (int column = 0; column < columnTypes.size(); column++) {
                BlockBuilder output = pageBuilder.getBlockBuilder(column);
                appendTo(columnTypes.get(column), record.getData(), output);
            }
        }
        return null;
    }

    private void appendTo(Type type, Object value, BlockBuilder output)
    {
        if (value == null) {
            output.appendNull();
            return;
        }

        Class<?> javaType = type.getJavaType();
        try {
            if (javaType == boolean.class) {
                type.writeBoolean(output, (Boolean) value);
            }
            else if (javaType == long.class) {
                if (type.equals(BIGINT)) {
                    type.writeLong(output, ((Number) value).longValue());
                }
                else if (type.equals(INTEGER)) {
                    type.writeLong(output, ((Number) value).intValue());
                }
                else if (type instanceof DecimalType decimalType) {
                    verify(decimalType.isShort(), "The type should be short decimal");
                    BigDecimal decimal = DECIMAL_CONVERTER.convert(decimalType.getPrecision(), decimalType.getScale(), value);
                    type.writeLong(output, encodeShortScaledValue(decimal, decimalType.getScale()));
                }
                else if (type.equals(DATE)) {
                    type.writeLong(output, ((Number) value).intValue());
                }
                else if (type.equals(TIMESTAMP_MICROS)) {
                    type.writeLong(output, toTrinoTimestamp(((Utf8) value).toString()));
                }
                else if (type.equals(TIME_MICROS)) {
                    type.writeLong(output, (long) value * PICOSECONDS_PER_MICROSECOND);
                }
                else {
                    throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
                }
            }
            else if (javaType == double.class) {
                type.writeDouble(output, ((Number) value).doubleValue());
            }
            else if (type.getJavaType() == Int128.class) {
                writeObject(output, type, value);
            }
            else if (javaType == Slice.class) {
                writeSlice(output, type, value);
            }
            else if (javaType == LongTimestampWithTimeZone.class) {
                verify(type.equals(TIMESTAMP_TZ_MICROS));
                long epochMicros = (long) value;
                int picosOfMillis = toIntExact(floorMod(epochMicros, MICROSECONDS_PER_MILLISECOND)) * PICOSECONDS_PER_MICROSECOND;
                type.writeObject(output, fromEpochMillisAndFraction(floorDiv(epochMicros, MICROSECONDS_PER_MILLISECOND), picosOfMillis, UTC_KEY));
            }
            else if (javaType == Block.class) {
                writeBlock(output, type, value);
            }
            else {
                throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unhandled type for %s: %s", javaType.getSimpleName(), type));
            }
        }
        catch (ClassCastException ignore) {
            // returns null instead of raising exception
            output.appendNull();
        }
    }

    private static void writeSlice(BlockBuilder output, Type type, Object value)
    {
        if (type instanceof VarcharType) {
            type.writeSlice(output, utf8Slice(((Utf8) value).toString()));
        }
        else if (type instanceof VarbinaryType) {
            if (value instanceof ByteBuffer) {
                type.writeSlice(output, Slices.wrappedBuffer((ByteBuffer) value));
            }
            else {
                output.appendNull();
            }
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Slice: " + type.getTypeSignature());
        }
    }

    private static void writeObject(BlockBuilder output, Type type, Object value)
    {
        if (type instanceof DecimalType decimalType) {
            verify(!decimalType.isShort(), "The type should be long decimal");
            BigDecimal decimal = DECIMAL_CONVERTER.convert(decimalType.getPrecision(), decimalType.getScale(), value);
            type.writeObject(output, Decimals.encodeScaledValue(decimal, decimalType.getScale()));
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Object: " + type.getTypeSignature());
        }
    }

    private void writeBlock(BlockBuilder output, Type type, Object value)
    {
        if (type instanceof ArrayType && value instanceof List<?>) {
            BlockBuilder builder = output.beginBlockEntry();

            for (Object element : (List<?>) value) {
                appendTo(type.getTypeParameters().get(0), element, builder);
            }

            output.closeEntry();
            return;
        }
        if (type instanceof RowType && value instanceof GenericRecord record) {
            BlockBuilder builder = output.beginBlockEntry();

            List<String> fieldNames = new ArrayList<>();
            for (int i = 0; i < type.getTypeSignature().getParameters().size(); i++) {
                TypeSignatureParameter parameter = type.getTypeSignature().getParameters().get(i);
                fieldNames.add(parameter.getNamedTypeSignature().getName().orElse("field" + i));
            }
            checkState(fieldNames.size() == type.getTypeParameters().size(), "fieldName doesn't match with type size : %s", type);
            for (int index = 0; index < type.getTypeParameters().size(); index++) {
                appendTo(type.getTypeParameters().get(index), record.get(fieldNames.get(index)), builder);
            }
            output.closeEntry();
            return;
        }
        throw new TrinoException(GENERIC_INTERNAL_ERROR, "Unhandled type for Block: " + type.getTypeSignature());
    }

    @Override
    public long getMemoryUsage()
    {
        return pageBuilder.getSizeInBytes();
    }

    @Override
    public void close()
            throws IOException
    {

    }

    static class AvroDecimalConverter
    {
        private static final Conversions.DecimalConversion AVRO_DECIMAL_CONVERSION = new Conversions.DecimalConversion();

        BigDecimal convert(int precision, int scale, Object value)
        {
            Schema schema = new Schema.Parser().parse(format("{\"type\":\"bytes\",\"logicalType\":\"decimal\",\"precision\":%d,\"scale\":%d}", precision, scale));
            return AVRO_DECIMAL_CONVERSION.fromBytes((ByteBuffer) value, schema, schema.getLogicalType());
        }
    }
}
