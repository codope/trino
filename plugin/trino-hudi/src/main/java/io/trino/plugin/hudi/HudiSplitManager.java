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
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;
import io.trino.plugin.base.classloader.ClassLoaderSafeConnectorSplitSource;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.util.ConfigurationUtils.toJobConf;
import static io.trino.plugin.hive.util.HiveUtil.getPartitionKeys;
import static io.trino.plugin.hudi.HudiSessionProperties.isHudiMetadataEnabled;
import static io.trino.plugin.hudi.HudiUtil.getMetaClient;
import static io.trino.plugin.hudi.HudiUtil.getPartitionSchema;
import static io.trino.plugin.hudi.HudiUtil.isHudiParquetInputFormat;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_LOCATION;
import static org.apache.hudi.common.table.timeline.TimelineUtils.getPartitionsWritten;

public class HudiSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(HudiSplitManager.class);

    private final HudiTransactionManager transactionManager;
    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public HudiSplitManager(HudiTransactionManager transactionManager, HdfsEnvironment hdfsEnvironment)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        log.warn(" >>>> Getting Splits <<<< ");
        HiveIdentity identity = new HiveIdentity(session);
        HudiTableHandle hudiTable = (HudiTableHandle) tableHandle;
        SchemaTableName tableName = hudiTable.getSchemaTableName();
        HiveMetastore metastore = transactionManager.get(transaction).getMetastore();
        Table table = metastore.getTable(identity, tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));
        HdfsEnvironment.HdfsContext context = new HdfsEnvironment.HdfsContext(session);
        FileSystem fs = null;
        try {
            fs = hdfsEnvironment.getFileSystem(context, new Path(table.getStorage().getLocation()));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        Configuration conf = hdfsEnvironment.getConfiguration(context, new Path(table.getStorage().getLocation()));
        HoodieTableMetaClient metaClient = hudiTable.getMetaClient().orElseGet(() -> getMetaClient(conf, hudiTable.getBasePath()));
        List<String> partitionValues = getPartitionsWritten(metaClient.getActiveTimeline());
        log.warn("Fetched partitions from Hudi: " + partitionValues);
        hudiTable.getPartitions().ifPresent(p -> p.forEach(p1 -> log.warn("Partitions from TableHandle: " + p1)));

        List<String> columnNames = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        log.warn("Column Names: " + columnNames);
        HudiSplitSource splitSource;
        String tablePath = table.getStorage().getLocation();
        Optional<FileStatus[]> fileStatuses = Optional.empty();
        if (!columnNames.isEmpty()) {
            List<List<String>> partitionNames = metastore.getPartitionNamesByFilter(identity, tableName.getSchemaName(), tableName.getTableName(), columnNames, TupleDomain.all())
                    .orElseThrow(() -> new TableNotFoundException(hudiTable.getSchemaTableName()))
                    .stream()
                    .map(HiveUtil::toPartitionValues)
                    .collect(toImmutableList());
            log.warn("Partition Names: " + partitionNames);

            ImmutableList.Builder<HivePartitionKey> partitionKeyBuilder = ImmutableList.builder();
            ImmutableList.Builder<FileStatus> fileStatusBuilder = ImmutableList.builder();
            Map<HivePartitionKey, Path> partitionKeyPath = new HashMap<>();
            for (List<String> partitionName : partitionNames) {
                Optional<Partition> partition1 = metastore.getPartition(identity, table, partitionName);
                Properties schema1 = getPartitionSchema(table, partition1);
                String dataDir1 = schema1.getProperty(META_TABLE_LOCATION);
                log.warn(">>> dataDir1: " + dataDir1);

                Optional<FileStatus[]> fileStatuses1 = getFileStatuses(fs, conf, dataDir1, Optional.empty(), schema1);
                fileStatuses1.ifPresent(fss -> fileStatusBuilder.addAll(Arrays.asList(fss)));
                List<HivePartitionKey> partitionKeys1 = getPartitionKeys(table, partition1);
                partitionKeys1.forEach(p -> log.warn(">>> Fetched partitions from HiveUtil: " + p));
                partitionKeys1.forEach(partitionKey -> partitionKeyPath.putIfAbsent(partitionKey, new Path(dataDir1)));
                partitionKeyBuilder.addAll(partitionKeys1);
            }

            splitSource = new HudiSplitSource(
                    hudiTable,
                    conf,
                    partitionKeyBuilder.build(),
                    isHudiMetadataEnabled(session),
                    Optional.of(Iterables.toArray(fileStatusBuilder.build(), FileStatus.class)),
                    tablePath,
                    partitionKeyPath);
        }
        else {
            // no partitions, so data dir is same as table path
            Properties schema = getPartitionSchema(table, Optional.empty());
            fileStatuses = getFileStatuses(fs, conf, tablePath, fileStatuses, schema);
            splitSource = new HudiSplitSource(hudiTable, conf, ImmutableList.of(), isHudiMetadataEnabled(session), fileStatuses, tablePath, ImmutableMap.of());
        }

        return new ClassLoaderSafeConnectorSplitSource(splitSource, Thread.currentThread().getContextClassLoader());
    }

    private static Optional<FileStatus[]> getFileStatuses(FileSystem fs, Configuration conf, String tablePath, Optional<FileStatus[]> fileStatuses, Properties schema)
    {
        InputFormat inputFormat = HiveUtil.getInputFormat(conf, schema, false);
        log.warn(">>> Check for inputFormat: " + isHudiParquetInputFormat(inputFormat));

        try {
            if (isHudiParquetInputFormat(inputFormat)) {
                fileStatuses = Optional.of(((HoodieParquetInputFormat) inputFormat).listStatus(toJobConf(conf)));
            }
            if (fileStatuses.isPresent()) {
                log.warn(">>> Total Files: " + fileStatuses.get().length);
                if (fileStatuses.get().length == 0 && fs != null) {
                    fileStatuses = Optional.of(fs.listStatus(new Path(tablePath)));
                    log.warn(">>> Total Files: " + fileStatuses.get().length);
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return fileStatuses;
    }
}
