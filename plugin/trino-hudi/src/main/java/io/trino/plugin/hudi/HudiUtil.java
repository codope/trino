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
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.PathWithBootstrapFileStatus;
import org.apache.hudi.hive.HiveStylePartitionValueExtractor;
import org.apache.hudi.hive.MultiPartKeysValueExtractor;
import org.apache.hudi.hive.PartitionValueExtractor;
import org.apache.hudi.hive.SlashEncodedDayPartitionValueExtractor;
import org.apache.hudi.hive.SlashEncodedHourPartitionValueExtractor;

import java.io.IOException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.util.HiveUtil.checkCondition;
import static io.trino.plugin.hive.util.HiveUtil.parsePartitionValue;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_INVALID_PARTITION_VALUE;
import static io.trino.spi.type.StandardTypes.BIGINT;
import static io.trino.spi.type.StandardTypes.BOOLEAN;
import static io.trino.spi.type.StandardTypes.DATE;
import static io.trino.spi.type.StandardTypes.DECIMAL;
import static io.trino.spi.type.StandardTypes.DOUBLE;
import static io.trino.spi.type.StandardTypes.INTEGER;
import static io.trino.spi.type.StandardTypes.REAL;
import static io.trino.spi.type.StandardTypes.SMALLINT;
import static io.trino.spi.type.StandardTypes.TIMESTAMP;
import static io.trino.spi.type.StandardTypes.TINYINT;
import static io.trino.spi.type.StandardTypes.VARBINARY;
import static io.trino.spi.type.StandardTypes.VARCHAR;
import static java.lang.Double.parseDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Float.parseFloat;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.common.FileUtils.unescapePathName;

public class HudiUtil
{
    private static final Logger log = Logger.get(HudiUtil.class);
    private static final double SPLIT_SLOP = 1.1;   // 10% slop

    private HudiUtil() {}

    public static HoodieTableMetaClient getMetaClient(Configuration conf, String basePath)
    {
        return HoodieTableMetaClient.builder().setConf(conf).setBasePath(basePath).build();
    }

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

    public static HudiPredicates splitPredicate(
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

        return new HudiPredicates(
                TupleDomain.withColumnDomains(partitionColumnPredicates),
                TupleDomain.withColumnDomains(regularColumnPredicates));
    }

    public static TupleDomain<HiveColumnHandle> mergePredicates(
            TupleDomain<HiveColumnHandle> predicates1, TupleDomain<HiveColumnHandle> predicates2)
    {
        Map<HiveColumnHandle, Domain> newColumnDomains = new HashMap<>();
        predicates1.getDomains().ifPresent(newColumnDomains::putAll);
        predicates2.getDomains().ifPresent(domains -> {
            for (HiveColumnHandle columnHandle : domains.keySet()) {
                if (newColumnDomains.containsKey(columnHandle)
                        && !newColumnDomains.get(columnHandle).equals(domains.get(columnHandle))) {
                    throw new HoodieIOException(String.format("Conflicting predicates for %s: [%s] and [%s]",
                            columnHandle, newColumnDomains.get(columnHandle), domains.get(columnHandle)));
                }
                else {
                    newColumnDomains.put(columnHandle, domains.get(columnHandle));
                }
            }
        });
        return TupleDomain.withColumnDomains(newColumnDomains);
    }

    public static boolean doesPartitionMatchPredicates(
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

    public static boolean doesPartitionMatchPredicates(
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

    public static HivePartition parsePartition(
            SchemaTableName tableName,
            String dummyPartitionName,
            List<String> partitionValues,
            List<HiveColumnHandle> partitionColumns,
            List<Type> partitionColumnTypes)
    {
        ImmutableMap.Builder<ColumnHandle, NullableValue> builder = ImmutableMap.builder();
        for (int i = 0; i < partitionColumns.size(); i++) {
            HiveColumnHandle column = partitionColumns.get(i);
            NullableValue parsedValue = parsePartitionValue(
                    dummyPartitionName, partitionValues.get(i), partitionColumnTypes.get(i));
            builder.put(column, parsedValue);
        }
        Map<ColumnHandle, NullableValue> values = builder.build();
        return new HivePartition(tableName, dummyPartitionName, values);
    }

    public static boolean partitionMatches(List<HiveColumnHandle> partitionColumns, TupleDomain<HiveColumnHandle> constraintSummary, HivePartition partition)
    {
        if (constraintSummary.isNone()) {
            log.warn("constraintSummary is none");
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

    public static Optional<Object> convertPartitionValue(
            String partitionColumnName,
            String partitionValue,
            TypeSignature partitionDataType)
    {
        if (isNull(partitionValue)) {
            return Optional.empty();
        }

        String baseType = partitionDataType.getBase();
        try {
            switch (baseType) {
                case TINYINT:
                case SMALLINT:
                case INTEGER:
                case BIGINT:
                    return Optional.of(parseLong(partitionValue));
                case REAL:
                    return Optional.of((long) floatToRawIntBits(parseFloat(partitionValue)));
                case DOUBLE:
                    return Optional.of(parseDouble(partitionValue));
                case VARCHAR:
                case VARBINARY:
                    return Optional.of(utf8Slice(partitionValue));
                case DATE:
                    return Optional.of(LocalDate.parse(partitionValue, DateTimeFormatter.ISO_LOCAL_DATE).toEpochDay());
                case TIMESTAMP:
                    return Optional.of(Timestamp.valueOf(partitionValue).toLocalDateTime().toEpochSecond(ZoneOffset.UTC) * 1_000);
                case BOOLEAN:
                    checkArgument(partitionValue.equalsIgnoreCase("true") || partitionValue.equalsIgnoreCase("false"));
                    return Optional.of(Boolean.valueOf(partitionValue));
                case DECIMAL:
                    return Optional.of(Decimals.parse(partitionValue).getObject());
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

    public static List<FileSplit> getSplits(FileSystem fs, FileStatus fileStatus)
            throws IOException
    {
        if (fileStatus.isDirectory()) {
            throw new IOException("Not a file: " + fileStatus.getPath());
        }

        Path path = fileStatus.getPath();
        long length = fileStatus.getLen();

        // generate splits
        List<FileSplit> splits = new ArrayList<>();
        if (length != 0) {
            BlockLocation[] blkLocations;
            if (fileStatus instanceof LocatedFileStatus) {
                blkLocations = ((LocatedFileStatus) fileStatus).getBlockLocations();
            }
            else {
                blkLocations = fs.getFileBlockLocations(fileStatus, 0, length);
            }
            if (isSplitable(path)) {
                long splitSize = fileStatus.getBlockSize();

                long bytesRemaining = length;
                while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
                    String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations, length - bytesRemaining);
                    splits.add(makeSplit(path, length - bytesRemaining, splitSize, splitHosts[0], splitHosts[1]));
                    bytesRemaining -= splitSize;
                }

                if (bytesRemaining != 0) {
                    String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations, length - bytesRemaining);
                    splits.add(makeSplit(path, length - bytesRemaining, bytesRemaining, splitHosts[0], splitHosts[1]));
                }
            }
            else {
                String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations, 0);
                splits.add(makeSplit(path, 0, length, splitHosts[0], splitHosts[1]));
            }
        }
        else {
            //Create empty hosts array for zero length files
            splits.add(makeSplit(path, 0, length, new String[0]));
        }
        return splits;
    }

    private static boolean isSplitable(Path filename)
    {
        return !(filename instanceof PathWithBootstrapFileStatus);
    }

    private static FileSplit makeSplit(Path file, long start, long length, String[] hosts)
    {
        return new FileSplit(file, start, length, hosts);
    }

    private static FileSplit makeSplit(Path file, long start, long length, String[] hosts, String[] inMemoryHosts)
    {
        return new FileSplit(file, start, length, hosts, inMemoryHosts);
    }

    private static String[][] getSplitHostsAndCachedHosts(BlockLocation[] blkLocations, long offset)
            throws IOException
    {
        int startIndex = getBlockIndex(blkLocations, offset);

        return new String[][] {blkLocations[startIndex].getHosts(),
                blkLocations[startIndex].getCachedHosts()};
    }

    private static int getBlockIndex(BlockLocation[] blkLocations, long offset)
    {
        for (int i = 0; i < blkLocations.length; i++) {
            // is the offset inside this block?
            if ((blkLocations[i].getOffset() <= offset) &&
                    (offset < blkLocations[i].getOffset() + blkLocations[i].getLength())) {
                return i;
            }
        }
        BlockLocation last = blkLocations[blkLocations.length - 1];
        long fileLength = last.getOffset() + last.getLength() - 1;
        throw new IllegalArgumentException("Offset " + offset +
                " is outside of file (0.." +
                fileLength + ")");
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

    public static List<String> buildPartitionValues(String partitionNames)
    {
        ImmutableList.Builder<String> values = ImmutableList.builder();
        String[] parts = partitionNames.split("=");
        if (parts.length == 1) {
            values.add(unescapePathName(partitionNames));
            return values.build();
        }
        if (parts.length == 2) {
            values.add(unescapePathName(parts[1]));
            return values.build();
        }
        for (int i = 1; i < parts.length; i++) {
            String val = parts[i];
            int j = val.lastIndexOf('/');
            if (j == -1) {
                values.add(unescapePathName(val));
            }
            else {
                values.add(unescapePathName(val.substring(0, j)));
            }
        }
        return values.build();
    }

    public static PartitionValueExtractor inferPartitionValueExtractor(
            String relativePartitionPath, List<String> expectedPartitionValues)
            throws HoodieIOException
    {
        // The order of extractors to try should not be changed
        List<PartitionValueExtractor> partitionValueExtractorList = new ArrayList<>();
        partitionValueExtractorList.add(new HiveStylePartitionValueExtractor());
        partitionValueExtractorList.add(new MultiPartKeysValueExtractor());
        partitionValueExtractorList.add(new SlashEncodedDayPartitionValueExtractor());
        partitionValueExtractorList.add(new SlashEncodedHourPartitionValueExtractor());

        for (PartitionValueExtractor partitionValueExtractor : partitionValueExtractorList) {
            try {
                List<String> extractedPartitionValues =
                        partitionValueExtractor.extractPartitionValuesInPath(relativePartitionPath);
                if (extractedPartitionValues.equals(expectedPartitionValues)) {
                    log.debug(String.format("Inferred %s to be the partition value extractor",
                            partitionValueExtractor.getClass().getName()));
                    return partitionValueExtractor;
                }
                else {
                    log.debug(String.format("Cannot use partition value extractor %s due to value mismatch " +
                                    "(expected: %s, actual: %s), trying the next option ...",
                            partitionValueExtractor.getClass().getName(), expectedPartitionValues,
                            extractedPartitionValues));
                }
            }
            catch (IllegalArgumentException e) {
                log.debug(String.format("Cannot use partition value extractor %s, trying the next option ...",
                        partitionValueExtractor.getClass().getName()));
            }
        }

        throw new HoodieIOException("Cannot infer the partition value extractor");
    }
}
