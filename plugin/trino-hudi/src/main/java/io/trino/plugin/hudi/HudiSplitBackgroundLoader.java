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
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.util.HiveUtil.getPartitionKeys;
import static java.util.Objects.requireNonNull;

public class HudiSplitBackgroundLoader
        implements Runnable
{
    private static final Logger LOG = Logger.get(HudiSplitBackgroundLoader.class);
    private final Configuration conf;
    private final HudiTableHandle tableHandle;
    private final HoodieTableMetaClient metaClient;
    private final boolean metadataEnabled;
    private final Table table;
    private final HiveIdentity identity;
    private final HiveMetastore metastore;
    private final ArrayDeque<ConnectorSplit> connectorSplitBuffer;
    private int initialBatchSize = 2;
    private int maxBatchSize = 16;
    private int batchSize = -1;

    public HudiSplitBackgroundLoader(
            Configuration conf,
            HudiTableHandle tableHandle,
            HoodieTableMetaClient metaClient,
            boolean metadataEnabled,
            Table table,
            HiveIdentity identity,
            HiveMetastore metastore,
            ArrayDeque<ConnectorSplit> connectorSplitBuffer)
    {
        this.conf = requireNonNull(conf, "conf is null");
        this.tableHandle = tableHandle;
        this.metaClient = metaClient;
        this.metadataEnabled = metadataEnabled;
        this.table = table;
        this.identity = identity;
        this.metastore = metastore;
        this.connectorSplitBuffer = connectorSplitBuffer;
    }

    @Override
    public void run()
    {
        HoodieTimer timerAll = new HoodieTimer().startTimer();
        HoodieTimer timer = new HoodieTimer().startTimer();
        // Load the timeline and file status only once
        HoodieEngineContext engineContext = new HoodieLocalEngineContext(conf);
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                .enable(metadataEnabled)
                .build();
        // Scan the file system to load the instants from timeline
        LOG.debug("Loading file system view for " + metaClient.getBasePath());
        HoodieTableFileSystemView fileSystemView = FileSystemViewManager.createInMemoryFileSystemView(
                engineContext, metaClient, metadataConfig);
        LOG.warn(String.format("Finish in %d ms to load table view", timer.endTimer()));

        timer = new HoodieTimer().startTimer();
        List<String> partitionColumnNames = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        LOG.warn("Column Names: " + partitionColumnNames);
        List<String> fullPartitionNames = new ArrayList<>();
        SchemaTableName tableName = tableHandle.getSchemaTableName();
        List<List<String>> partitionNamesList;
        Iterator<List<String>> partitionNames;
        if (!partitionColumnNames.isEmpty()) {
            fullPartitionNames = metastore.getPartitionNamesByFilter(identity, tableName.getSchemaName(), tableName.getTableName(), partitionColumnNames, TupleDomain.all())
                    .orElseThrow(() -> new TableNotFoundException(tableHandle.getSchemaTableName()));
            partitionNamesList = fullPartitionNames
                    .stream()
                    .map(HiveUtil::toPartitionValues)
                    .collect(toImmutableList());
        }
        else {
            // no partitions, so data dir is same as table path
            partitionNamesList = Collections.singletonList(Collections.singletonList(""));
        }

        partitionNames = partitionNamesList.iterator();

        LOG.warn(String.format("Finish in %d ms. Partition Names: %s", timer.endTimer(), fullPartitionNames));
        LOG.warn(String.format("Partition Name elements: %s", partitionNamesList));

        Map<String, List<HivePartitionKey>> partitionMap = new HashMap<>();
        FileSystem fileSystem = metaClient.getFs();

        long currTime = System.currentTimeMillis();

        partitionNamesList.stream().parallel()
                .map(names -> {
                    Pair<String, List<HivePartitionKey>> partitionPathToKey = getPartitionPathToKey(
                            identity, metastore, table, table.getStorage().getLocation(), partitionColumnNames, names);
                    String relativePartitionPath = partitionPathToKey.getKey();
                    List<HivePartitionKey> partitionKeys = partitionPathToKey.getValue();
                    Stream<HoodieBaseFile> baseFiles = fileSystemView.getLatestBaseFiles(relativePartitionPath);
                    List<HudiSplit> hudiSplitsToAdd = baseFiles.parallel()
                            .flatMap(baseFile -> {
                                List<HudiSplit> hudiSplits = new ArrayList<>();
                                try {
                                    hudiSplits = HudiUtil.getSplits(
                                                    fileSystem, HoodieInputFormatUtils.getFileStatus(baseFile))
                                            .stream()
                                            .flatMap(fileSplit -> {
                                                List<HudiSplit> result = new ArrayList<>();
                                                try {
                                                    result.add(new HudiSplit(
                                                            fileSplit.getPath().toString(),
                                                            fileSplit.getStart(),
                                                            fileSplit.getLength(),
                                                            metaClient.getFs().getLength(fileSplit.getPath()),
                                                            ImmutableList.of(),
                                                            tableHandle.getPredicate(),
                                                            partitionMap.get(relativePartitionPath)));
                                                }
                                                catch (IOException e) {
                                                    throw new HoodieIOException(String.format(
                                                            "Unable to get Hudi split for %s, start=%d len=%d",
                                                            baseFile.getPath(), fileSplit.getStart(), fileSplit.getLength()), e);
                                                }
                                                return result.stream();
                                            })
                                            .collect(Collectors.toList());
                                }
                                catch (IOException e) {
                                    throw new HoodieIOException("Unable to get splits for " + baseFile.getPath(), e);
                                }
                                return hudiSplits.stream();
                            })
                            .collect(Collectors.toList());
                    synchronized (connectorSplitBuffer) {
                        connectorSplitBuffer.addAll(hudiSplitsToAdd);
                        LOG.warn(String.format("Time %d ms, adding %d splits. Total: %d splits",
                                System.currentTimeMillis() - currTime, hudiSplitsToAdd.size(), connectorSplitBuffer.size()));
                    }
                    return true;
                })
                .collect(Collectors.toList());

        /*
        while (partitionNames.hasNext()) {
            timer = new HoodieTimer().startTimer();

            List<List<String>> batchPartitionNames = new ArrayList<>();
            int count = updateBatchSize();
            LOG.warn(String.format("*** Batch size: %d ***", count));
            while (count > 0 && partitionNames.hasNext()) {
                batchPartitionNames.add(partitionNames.next());
                count--;
            }

            Map<String, List<HivePartitionKey>> batchKeyMap =
                    batchPartitionNames.stream().parallel()
                            .map(names -> getPartitionPathToKey(
                                    identity, metastore, table, table.getStorage().getLocation(), partitionColumnNames, names))
                            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
            partitionMap.putAll(batchKeyMap);

            LOG.warn(String.format("Finish in %d ms to get partition keys: %s", timer.endTimer(), batchKeyMap.toString()));

            timer = new HoodieTimer().startTimer();

            List<Pair<HoodieBaseFile, String>> baseFilePairs = batchKeyMap.keySet().stream().parallel()
                    .flatMap(relativePartitionPath -> fileSystemView.getLatestBaseFiles(relativePartitionPath)
                            .map(baseFile -> new ImmutablePair<>(baseFile, relativePartitionPath))
                            .collect(Collectors.toList()).stream())
                    .collect(Collectors.toList());

            // TODO: skip partitions that are filtered out based on the predicate
            LOG.warn(String.format("Finish in %d ms to get base files", timer.endTimer()));

            timer = new HoodieTimer().startTimer();
            List<HudiSplit> hudiSplitsToAdd = baseFilePairs.stream().parallel()
                    .flatMap(baseFilePair -> {
                        List<HudiSplit> hudiSplits = new ArrayList<>();
                        HoodieBaseFile baseFile = baseFilePair.getKey();
                        String partitionPath = baseFilePair.getValue();
                        try {
                            hudiSplits = HudiUtil.getSplits(
                                            fileSystem, HoodieInputFormatUtils.getFileStatus(baseFile))
                                    .stream()
                                    .flatMap(fileSplit -> {
                                        List<HudiSplit> result = new ArrayList<>();
                                        try {
                                            result.add(new HudiSplit(
                                                    fileSplit.getPath().toString(),
                                                    fileSplit.getStart(),
                                                    fileSplit.getLength(),
                                                    metaClient.getFs().getLength(fileSplit.getPath()),
                                                    ImmutableList.of(),
                                                    tableHandle.getPredicate(),
                                                    partitionMap.get(partitionPath)));
                                        }
                                        catch (IOException e) {
                                            throw new HoodieIOException(String.format(
                                                    "Unable to get Hudi split for %s, start=%d len=%d",
                                                    baseFile.getPath(), fileSplit.getStart(), fileSplit.getLength()), e);
                                        }
                                        return result.stream();
                                    })
                                    .collect(Collectors.toList());
                        }
                        catch (IOException e) {
                            throw new HoodieIOException("Unable to get splits for " + baseFile.getPath(), e);
                        }
                        return hudiSplits.stream();
                    })
                    .collect(Collectors.toList());

            LOG.warn(String.format("Finish in %d ms to get batch splits", timer.endTimer()));

            synchronized (connectorSplitBuffer) {
                connectorSplitBuffer.addAll(hudiSplitsToAdd);
            }
        }*/
        fileSystemView.close();
        LOG.warn(String.format("Finish getting all splits in %d ms", timerAll.endTimer()));
    }

    private int updateBatchSize()
    {
        // Start with smaller batch size to give first set of results quickly
        if (batchSize <= 0) {
            batchSize = initialBatchSize;
        }
        else if (batchSize < maxBatchSize) {
            batchSize *= 2;
            if (batchSize > maxBatchSize) {
                batchSize = maxBatchSize;
            }
        }
        return batchSize;
    }

    private Pair<String, List<HivePartitionKey>> getPartitionPathToKey(
            HiveIdentity identity,
            HiveMetastore metastore,
            Table table,
            String tablePath,
            List<String> columnNames,
            List<String> partitionName)
    {
        String relativePartitionPath = "";
        List<HivePartitionKey> partitionKeys = new ArrayList<>();
        if (!columnNames.isEmpty()) {
            Optional<Partition> partition1 = metastore.getPartition(identity, table, partitionName);
            String dataDir1 = partition1.isPresent()
                    ? partition1.get().getStorage().getLocation()
                    : tablePath;
            LOG.warn(">>> basePath: %s,  dataDir1: %s", tablePath, dataDir1);
            relativePartitionPath = FSUtils.getRelativePartitionPath(new Path(tablePath), new Path(dataDir1));
            partitionKeys = getPartitionKeys(table, partition1);
        }
        return new ImmutablePair<>(relativePartitionPath, partitionKeys);
    }
}
