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
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.PathWithBootstrapFileStatus;

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
    private static final double SPLIT_SLOP = 1.1;   // 10% slop

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

    public static List<FileSplit> getSplits(FileSystem fs, FileStatus fileStatus) throws IOException
    {
        if (fileStatus.isDirectory()) {
            throw new IOException("Not a file: " + fileStatus.getPath());
        }

        Path path = fileStatus.getPath();
        long length = fileStatus.getLen();

        // generate splits
        List<FileSplit> splits = new ArrayList<>();
        NetworkTopology clusterMap = new NetworkTopology();
        if (length != 0) {
            BlockLocation[] blkLocations;
            if (fileStatus instanceof LocatedFileStatus) {
                blkLocations = ((LocatedFileStatus) fileStatus).getBlockLocations();
            }
            else {
                blkLocations = fs.getFileBlockLocations(fileStatus, 0, length);
            }
            if (isSplitable(fs, path)) {
                long splitSize = fileStatus.getBlockSize();

                long bytesRemaining = length;
                while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
                    String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations,
                            length - bytesRemaining, splitSize, clusterMap);
                    splits.add(makeSplit(path, length - bytesRemaining, splitSize,
                            splitHosts[0], splitHosts[1]));
                    bytesRemaining -= splitSize;
                }

                if (bytesRemaining != 0) {
                    String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations, length
                            - bytesRemaining, bytesRemaining, clusterMap);
                    splits.add(makeSplit(path, length - bytesRemaining, bytesRemaining,
                            splitHosts[0], splitHosts[1]));
                }
            }
            else {
                String[][] splitHosts = getSplitHostsAndCachedHosts(blkLocations, 0, length, clusterMap);
                splits.add(makeSplit(path, 0, length, splitHosts[0], splitHosts[1]));
            }
        }
        else {
            //Create empty hosts array for zero length files
            splits.add(makeSplit(path, 0, length, new String[0]));
        }
        return splits;
    }

    private static boolean isSplitable(FileSystem fs, Path filename)
    {
        return !(filename instanceof PathWithBootstrapFileStatus);
    }

    private static long computeSplitSize(long goalSize, long minSize,
                          long blockSize)
    {
        return Math.max(minSize, Math.min(goalSize, blockSize));
    }

    private static FileSplit makeSplit(Path file, long start, long length,
                                  String[] hosts)
    {
        return new FileSplit(file, start, length, hosts);
    }

    private static FileSplit makeSplit(Path file, long start, long length,
                                  String[] hosts, String[] inMemoryHosts)
    {
        return new FileSplit(file, start, length, hosts, inMemoryHosts);
    }

    private static String[][] getSplitHostsAndCachedHosts(BlockLocation[] blkLocations,
                                                   long offset, long splitSize, NetworkTopology clusterMap)
            throws IOException
    {
        int startIndex = getBlockIndex(blkLocations, offset);

        return new String[][]{blkLocations[startIndex].getHosts(),
                    blkLocations[startIndex].getCachedHosts()};
    }

    private static int getBlockIndex(BlockLocation[] blkLocations,
                                long offset)
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
}
