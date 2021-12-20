/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TypeSignature;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getHiveSchema;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_INVALID_PARTITION_VALUE;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.parseFloat;
import static java.lang.Long.parseLong;
import static java.lang.String.format;

public class HudiUtil
{
    private static final Logger log = Logger.get(HudiUtil.class);

    private HudiUtil() {}

    public static HoodieTableMetaClient getMetaClient(Configuration conf, String basePath)
    {
        return HoodieTableMetaClient.builder().setConf(conf).setBasePath(basePath).build();
    }

    public static boolean isHudiParquetInputFormat(InputFormat<?, ?> inputFormat)
    {
        return inputFormat instanceof HoodieParquetInputFormat;
    }

    public static Properties getPartitionSchema(Table table, Optional<Partition> partition)
    {
        if (partition.isEmpty()) {
            return getHiveSchema(table);
        }
        return getHiveSchema(partition.get(), table);
    }

    public static List<TupleDomain<ColumnHandle>> splitPredicate(
            TupleDomain<ColumnHandle> predicate)
    {
        Map<ColumnHandle, Domain> partitionColumnPredicates = new HashMap<>();
        Map<ColumnHandle, Domain> regularColumnPredicates = new HashMap<>();

        Optional<Map<ColumnHandle, Domain>> domains = predicate.getDomains();
        domains.ifPresent(columnHandleDomainMap -> columnHandleDomainMap.forEach((key, value) -> {
            HiveColumnHandle columnHandle = (HiveColumnHandle) key;
            if (columnHandle.isPartitionKey()) {
                partitionColumnPredicates.put(key, value);
            }
            else {
                regularColumnPredicates.put(key, value);
            }
        }));

        return ImmutableList.of(
                TupleDomain.withColumnDomains(partitionColumnPredicates),
                TupleDomain.withColumnDomains(regularColumnPredicates));
    }

    public static Object convertPartitionValue(
            String partitionColumnName,
            String partitionValue,
            TypeSignature partitionDataType)
    {
        log.warn(">>> convertPartitionValue column: %s, value: %s, dataType: %s", partitionColumnName, partitionValue, partitionDataType);
        if (partitionValue == null) {
            return null;
        }

        String typeBase = partitionDataType.getBase();
        log.warn(">>> Base Type: %s", typeBase);
        try {
            switch (typeBase) {
                case StandardTypes.TINYINT:
                case StandardTypes.SMALLINT:
                case StandardTypes.INTEGER:
                case StandardTypes.BIGINT:
                    return parseLong(partitionValue);
                case StandardTypes.REAL:
                    return (long) floatToRawIntBits(parseFloat(partitionValue));
                case StandardTypes.DOUBLE:
                    return parseDouble(partitionValue);
                case StandardTypes.VARCHAR:
                case StandardTypes.VARBINARY:
                    return utf8Slice(partitionValue);
                case StandardTypes.DATE:
                    return LocalDate.parse(partitionValue, DateTimeFormatter.ISO_LOCAL_DATE).toEpochDay();
                case StandardTypes.TIMESTAMP:
                    return Timestamp.valueOf(partitionValue).toLocalDateTime().toEpochSecond(ZoneOffset.UTC) * 1_000;
                case StandardTypes.BOOLEAN:
                    checkArgument(partitionValue.equalsIgnoreCase("true") || partitionValue.equalsIgnoreCase("false"));
                    return Boolean.valueOf(partitionValue);
                case StandardTypes.DECIMAL:
                    return Decimals.parse(partitionValue).getObject();
                default:
                    throw new TrinoException(HUDI_INVALID_PARTITION_VALUE,
                            format("Unsupported data type '%s' for partition column %s", partitionDataType, partitionColumnName));
            }
        }
        catch (IllegalArgumentException | DateTimeParseException e) {
            throw new TrinoException(HUDI_INVALID_PARTITION_VALUE,
                    format("Can not parse partition value '%s' of type '%s' for partition column '%s'",
                            partitionValue, partitionDataType, partitionColumnName));
        }
    }
}
