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
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import javax.inject.Inject;

import java.util.HashMap;
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
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_LOCATION;
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
        String tablePath = table.getStorage().getLocation();
        Configuration conf = hdfsEnvironment.getConfiguration(context, new Path(tablePath));
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
        Map<String, List<HivePartitionKey>> partitionMap = new HashMap<>();
        if (!columnNames.isEmpty()) {
            List<List<String>> partitionNames = metastore.getPartitionNamesByFilter(identity, tableName.getSchemaName(), tableName.getTableName(), columnNames, TupleDomain.all())
                    .orElseThrow(() -> new TableNotFoundException(hudiTable.getSchemaTableName()))
                    .stream()
                    .map(HiveUtil::toPartitionValues)
                    .collect(toImmutableList());
            log.warn("Partition Names: " + partitionNames);

            for (List<String> partitionName : partitionNames) {
                Optional<Partition> partition1 = metastore.getPartition(identity, table, partitionName);
                Properties schema1 = getPartitionSchema(table, partition1);
                String dataDir1 = schema1.getProperty(META_TABLE_LOCATION);
                log.warn(">>> basePath: %s,  dataDir1: %s", tablePath, dataDir1);
                String relativePartitionPath = FSUtils.getRelativePartitionPath(new Path(tablePath), new Path(dataDir1));
                List<HivePartitionKey> partitionKeys1 = getPartitionKeys(table, partition1);
                partitionMap.putIfAbsent(relativePartitionPath, partitionKeys1);
                partitionKeys1.forEach(p -> log.warn(">>> Fetched partitions from HiveUtil: " + p));
            }
        }
        else {
            // no partitions, so data dir is same as table path
            partitionMap.put("", ImmutableList.of());
        }

        log.debug("Partition map: " + partitionMap);
        splitSource = new HudiSplitSource(session, hudiTable, conf, partitionMap,
                isHudiMetadataEnabled(session));
        return new ClassLoaderSafeConnectorSplitSource(splitSource, Thread.currentThread().getContextClassLoader());
    }

    void printConf(Configuration conf)
    {
        for (Map.Entry<String, String> entry : conf) {
            log.warn("%s=%s\n", entry.getKey(), entry.getValue());
        }
    }
}
