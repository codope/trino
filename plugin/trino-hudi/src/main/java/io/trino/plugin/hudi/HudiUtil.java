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
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartition;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.HivePartitionManager;
import io.trino.plugin.hive.metastore.Column;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hive.HiveStylePartitionValueExtractor;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.hive.PartitionValueExtractor;
import org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor;
import org.apache.hudi.hive.SlashEncodedHourPartitionValueExtractor;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.util.HiveUtil.checkCondition;
import static io.trino.plugin.hive.util.HiveUtil.parsePartitionValue;
import static java.util.stream.Collectors.toList;

public final class HudiUtil
{
    private static final Logger log = Logger.get(HudiUtil.class);
    private static final List<PartitionValueExtractor> PARTITION_VALUE_EXTRACTORS;

    static {
        PARTITION_VALUE_EXTRACTORS = ImmutableList.<PartitionValueExtractor>builder()
                .add(new HiveStylePartitionValueExtractor())
                .add(new MultiPartKeysValueExtractor())
                .add(new SlashEncodedDayPartitionValueExtractor())
                .add(new SlashEncodedHourPartitionValueExtractor())
                .build();
    }

    private HudiUtil() {}

    public static boolean isHudiParquetInputFormat(InputFormat<?, ?> inputFormat)
    {
        return inputFormat instanceof HoodieParquetInputFormat;
    }

    public static HoodieFileFormat getHudiFileFormat(String path)
    {
        final String extension = FSUtils.getFileExtension(path);
        if (extension.equals(HoodieFileFormat.PARQUET.getFileExtension())) {
            return HoodieFileFormat.PARQUET;
        }
        if (extension.equals(HoodieFileFormat.HOODIE_LOG.getFileExtension())) {
            return HoodieFileFormat.HOODIE_LOG;
        }
        if (extension.equals(HoodieFileFormat.ORC.getFileExtension())) {
            return HoodieFileFormat.ORC;
        }
        if (extension.equals(HoodieFileFormat.HFILE.getFileExtension())) {
            return HoodieFileFormat.HFILE;
        }
        throw new HoodieIOException("Hoodie InputFormat not implemented for base file of type " + extension);
    }

    public static boolean partitionMatchesPredicates(
            SchemaTableName tableName,
            String hivePartitionName,
            List<HiveColumnHandle> partitionColumnHandles,
            TupleDomain<HiveColumnHandle> constraintSummary)
    {
        List<Type> partitionColumnTypes = partitionColumnHandles.stream()
                .map(HiveColumnHandle::getType)
                .collect(toList());
        HivePartition partition = HivePartitionManager.parsePartition(
                tableName, hivePartitionName, partitionColumnHandles, partitionColumnTypes);

        return partitionMatches(partitionColumnHandles, constraintSummary, partition);
    }

    public static boolean partitionMatchesPredicates(
            SchemaTableName tableName,
            String relativePartitionPath,
            List<String> partitionValues,
            List<HiveColumnHandle> partitionColumnHandles,
            TupleDomain<HiveColumnHandle> constraintSummary)
    {
        List<Type> partitionColumnTypes = partitionColumnHandles.stream()
                .map(HiveColumnHandle::getType)
                .collect(toList());
        HivePartition partition = parsePartition(
                tableName, relativePartitionPath, partitionValues, partitionColumnHandles, partitionColumnTypes);

        return partitionMatches(partitionColumnHandles, constraintSummary, partition);
    }

    private static HivePartition parsePartition(
            SchemaTableName tableName,
            String partitionName,
            List<String> partitionValues,
            List<HiveColumnHandle> partitionColumns,
            List<Type> partitionColumnTypes)
    {
        ImmutableMap.Builder<ColumnHandle, NullableValue> builder = ImmutableMap.builder();
        for (int i = 0; i < partitionColumns.size(); i++) {
            HiveColumnHandle column = partitionColumns.get(i);
            NullableValue parsedValue = parsePartitionValue(
                    partitionName, partitionValues.get(i), partitionColumnTypes.get(i));
            builder.put(column, parsedValue);
        }
        Map<ColumnHandle, NullableValue> values = builder.buildOrThrow();
        return new HivePartition(tableName, partitionName, values);
    }

    public static boolean partitionMatches(List<HiveColumnHandle> partitionColumns, TupleDomain<HiveColumnHandle> constraintSummary, HivePartition partition)
    {
        if (constraintSummary.isNone()) {
            return false;
        }
        Map<HiveColumnHandle, Domain> domains = constraintSummary.getDomains().orElseGet(ImmutableMap::of);
        for (HiveColumnHandle column : partitionColumns) {
            NullableValue value = partition.getKeys().get(column);
            Domain allowedDomain = domains.get(column);
            if (allowedDomain != null && !allowedDomain.includesNullableValue(value.getValue())) {
                return false;
            }
        }
        return true;
    }

    public static List<HivePartitionKey> buildPartitionKeys(List<Column> keys, List<String> values)
    {
        checkCondition(keys.size() == values.size(), HIVE_INVALID_METADATA,
                "Expected %s partition key values, but got %s. Keys: %s, Values: %s.",
                keys.size(), values.size(), keys, values);
        ImmutableList.Builder<HivePartitionKey> partitionKeys = ImmutableList.builder();
        for (int i = 0; i < keys.size(); i++) {
            String name = keys.get(i).getName();
            String value = values.get(i);
            partitionKeys.add(new HivePartitionKey(name, value));
        }
        return partitionKeys.build();
    }

    public static PartitionValueExtractor inferPartitionValueExtractor(
            String relativePartitionPath,
            List<String> expectedPartitionValues)
            throws HoodieIOException
    {
        for (PartitionValueExtractor partitionValueExtractor : PARTITION_VALUE_EXTRACTORS) {
            try {
                List<String> extractedPartitionValues =
                        partitionValueExtractor.extractPartitionValuesInPath(relativePartitionPath);
                if (extractedPartitionValues.equals(expectedPartitionValues)) {
                    log.debug("Inferred %s to be the partition value extractor",
                            partitionValueExtractor.getClass().getName());
                    return partitionValueExtractor;
                }
                else {
                    log.debug("Cannot use partition value extractor %s due to value mismatch (expected: %s, actual: %s), trying the next option ...",
                            partitionValueExtractor.getClass().getName(),
                            expectedPartitionValues,
                            extractedPartitionValues);
                }
            }
            catch (IllegalArgumentException e) {
                log.debug("Cannot use partition value extractor %s, trying the next option ...",
                        partitionValueExtractor.getClass().getName());
            }
        }

        throw new HoodieIOException("Cannot infer the partition value extractor");
    }
}
