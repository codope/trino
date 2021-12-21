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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import javax.inject.Inject;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.util.HiveUtil.getPartitionKeys;
import static io.trino.plugin.hudi.HudiSessionProperties.isHudiMetadataEnabled;
import static io.trino.plugin.hudi.HudiUtil.getMetaClient;
import static io.trino.plugin.hudi.HudiUtil.getPartitionSchema;
import static java.util.Objects.requireNonNull;
import static org.apache.hudi.common.table.timeline.TimelineUtils.getPartitionsWritten;

public class HudiSplitManager
        implements ConnectorSplitManager
{
    public static final Pattern HOODIE_CONSUME_MODE_PATTERN_STRING = Pattern.compile("hoodie\\.(.*)\\.consume\\.mode");
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
        Map<String, String> valByRegex = conf.getValByRegex(HOODIE_CONSUME_MODE_PATTERN_STRING.pattern());
        log.debug("Hoodie consume mode: " + valByRegex);
        HoodieTableMetaClient metaClient = hudiTable.getMetaClient().orElseGet(() -> getMetaClient(conf, hudiTable.getBasePath()));
        List<String> partitionValues = getPartitionsWritten(metaClient.getActiveTimeline());
        log.debug("HudiSplitManager ref: " + this.toString());
        log.debug("Table ref: " + table.toString());
        log.debug("HoodieTableMetaClient ref: " + metaClient.toString());
        log.debug("HoodieTableMetaClient base path: " + metaClient.getBasePath());
        log.warn("Fetched partitions from Hudi: " + partitionValues);
        hudiTable.getPartitions().ifPresent(p -> p.forEach(p1 -> log.warn("Partitions from TableHandle: " + p1)));

        List<String> columnNames = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        log.warn("Column Names: " + columnNames);
        HudiSplitSource splitSource;
        String tablePath = table.getStorage().getLocation();
        List<HivePartitionKey> partitionKeys = ImmutableList.of();
        Properties schema;
        Iterator<String> relativePartitionPaths;
        if (!columnNames.isEmpty()) {
            List<List<String>> partitionNames = metastore.getPartitionNamesByFilter(identity, tableName.getSchemaName(), tableName.getTableName(), columnNames, TupleDomain.all())
                    .orElseThrow(() -> new TableNotFoundException(hudiTable.getSchemaTableName()))
                    .stream()
                    .map(HiveUtil::toPartitionValues)
                    .collect(toImmutableList());
            log.warn("Partition Names: " + partitionNames);

            Optional<Partition> partition = metastore.getPartition(identity, table, partitionNames.get(0));
            schema = getPartitionSchema(table, partition);

            log.warn("Fetched partitions from Metastore: " + partition.get());
            log.warn("Partition schema: " + schema);

            partitionKeys = getPartitionKeys(table, partition);
            partitionKeys.forEach(p -> log.warn("Fetched partitions from HiveUtil: " + p));

            // InputFormat inputFormat = HiveUtil.getInputFormat(conf, schema, false);
            // log.debug(">>> Check for inputFormat: " + isHudiParquetInputFormat(inputFormat));
            // log.debug(">>> Conf: ");
            // printConf(conf);
        }
        else {
            // no partitions, so data dir is same as table path
            schema = getPartitionSchema(table, Optional.empty());
            partitionValues = ImmutableList.of("");
        }

        /*
        // Non Hudi table should also be compatible with HoodieParquetInputFormat
        HoodieParquetInputFormat inputFormat = new HoodieParquetInputFormat();
        inputFormat.setConf(conf);

        JobConf jobConf = toJobConf(conf);
        String allAbsolutePartitionPaths = partitionValues.stream().map(
                relativePartitionPath -> FSUtils.getPartitionPath(tablePath, relativePartitionPath).toString()
        ).collect(Collectors.joining(","));
        FileInputFormat.setInputPaths(jobConf, allAbsolutePartitionPaths);
        // Pass SerDes and Table parameters into input format configuration
        fromProperties(schema).forEach(jobConf::set);
        String dataDir = schema.getProperty(META_TABLE_LOCATION);
        try {
            InputSplit[] inputSplits = inputFormat.getSplits(jobConf, 0);
            log.warn(">>> Total Splits: " + inputSplits.length);
            splitSource = new HudiSplitSource(session, hudiTable, conf, partitionKeys, isHudiMetadataEnabled(session), inputSplits, tablePath, dataDir);
            return new ClassLoaderSafeConnectorSplitSource(splitSource, Thread.currentThread().getContextClassLoader());
        }
        catch (IOException e) {
            throw new HoodieIOException("Error getting input splits", e);
        }
        */
        relativePartitionPaths = partitionValues.iterator();
        splitSource = new HudiSplitSource(session, hudiTable, conf, partitionKeys, relativePartitionPaths,
                isHudiMetadataEnabled(session), tablePath);
        return new ClassLoaderSafeConnectorSplitSource(splitSource, Thread.currentThread().getContextClassLoader());
    }

    void printConf(Configuration conf)
    {
        for (Map.Entry<String, String> entry : conf) {
            log.warn("%s=%s\n", entry.getKey(), entry.getValue());
        }
    }
}
