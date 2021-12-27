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
import com.google.common.collect.Iterators;
import io.airlift.log.Logger;
import io.trino.plugin.hive.HivePartition;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.spi.connector.ConnectorPartitionHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilter;
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
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.util.HiveUtil.getPartitionKeys;
import static io.trino.plugin.hudi.HudiUtil.buildPartitionKeys;
import static io.trino.plugin.hudi.HudiUtil.buildPartitionValues;
import static io.trino.plugin.hudi.HudiUtil.getMetaClient;
import static io.trino.plugin.hudi.HudiUtil.getSplits;
import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class HudiSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(HudiSplitSource.class);

    private final HiveIdentity identity;
    private final HiveMetastore metastore;
    private final SchemaTableName tableName;
    private final Configuration conf;
    private final HudiTableHandle tableHandle;
    private final Table table;
    private final FileSystem fileSystem;
    private final HoodieTableMetaClient metaClient;
    private final Map<String, List<HivePartitionKey>> partitionMap;
    private final boolean metadataEnabled;
    private final boolean shouldSkipMetaStoreForPartition;
    private final Map<HoodieBaseFile, String> baseFileToPartitionMap;
    private final ArrayDeque<HoodieBaseFile> baseFiles = new ArrayDeque<>();
    private final DynamicFilter dynamicFilter;
    private final int partitionBatchNum = 32;

    private HoodieTableFileSystemView fileSystemView;
    private List<Column> partitionColumns;
    private Iterator<List<String>> metastorePartitions;
    private Iterator<String> tableHandlePartitions;

    public HudiSplitSource(
            HiveIdentity identity,
            HiveMetastore metastore,
            HudiTableHandle tableHandle,
            Configuration conf,
            boolean metadataEnabled,
            boolean shouldSkipMetaStoreForPartition,
            DynamicFilter dynamicFilter)
    {
        this.identity = identity;
        this.metastore = metastore;
        this.tableName = tableHandle.getSchemaTableName();
        this.table = metastore.getTable(identity, tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));
        this.conf = requireNonNull(conf, "conf is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.partitionMap = new HashMap<>();
        this.metadataEnabled = metadataEnabled;
        this.shouldSkipMetaStoreForPartition = shouldSkipMetaStoreForPartition;
        this.metaClient = tableHandle.getMetaClient().orElseGet(() -> getMetaClient(conf, tableHandle.getBasePath()));
        this.fileSystem = metaClient.getFs();
        this.baseFileToPartitionMap = new HashMap<>();
        this.dynamicFilter = dynamicFilter;
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        log.debug("Dynamic filter: " + dynamicFilter.getColumnsCovered());
        log.debug("Getting next batch with partitionKeys: " + partitionMap.keySet());
        try {
            List<ConnectorSplit> connectorSplits = getSplitsForSnapshotMode(maxSize);
            return completedFuture(new ConnectorSplitBatch(connectorSplits, isFinished()));
        }
        catch (IOException e) {
            throw new HoodieIOException("Failed to get next batch of splits", e);
        }
    }

    @Override
    public void close()
    {
        if (nonNull(fileSystem)) {
            fileSystemView.close();
        }
    }

    @Override
    public boolean isFinished()
    {
        if (shouldSkipMetaStoreForPartition) {
            return !tableHandlePartitions.hasNext() && baseFiles.isEmpty();
        }
        return !metastorePartitions.hasNext() && baseFiles.isEmpty();
    }

    private List<ConnectorSplit> getSplitsForSnapshotMode(int maxSize)
            throws IOException
    {
        if (isNull(this.fileSystemView)) {
            HoodieTimer timer = new HoodieTimer().startTimer();
            // First time calling this
            // Load the timeline and file status only once
            HoodieEngineContext engineContext = new HoodieLocalEngineContext(conf);
            HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                    .enable(metadataEnabled)
                    .build();
            // Scan the file system to load the instants from timeline
            log.debug("Loading file system view for " + metaClient.getBasePath());
            this.fileSystemView = FileSystemViewManager.createInMemoryFileSystemView(engineContext, metaClient, metadataConfig);
            log.debug(String.format("Finish in %d ms to load table view", timer.endTimer()));

            timer = new HoodieTimer().startTimer();
            partitionColumns = table.getPartitionColumns();

            if (!partitionColumns.isEmpty()) {
                if (shouldSkipMetaStoreForPartition) {
                    List<String> partitions = tableHandle.getPartitions().orElseGet(ImmutableList::of).stream()
                            .map(HivePartition::getPartitionId).collect(Collectors.toList());
                    log.debug(">>> Partitions tableHandle: %s", partitions);
                    tableHandlePartitions = partitions.iterator();
                }
                else {
                    List<String> fullPartitionNames = metastore.getPartitionNamesByFilter(
                                    identity,
                                    tableName.getSchemaName(),
                                    tableName.getTableName(),
                                    partitionColumns.stream()
                                            .map(Column::getName)
                                            .collect(Collectors.toList()),
                                    TupleDomain.all())
                            .orElseThrow(() -> new TableNotFoundException(tableHandle.getSchemaTableName()));
                    List<List<String>> partitionNameElements = fullPartitionNames
                            .stream()
                            .map(HiveUtil::toPartitionValues)
                            .collect(toImmutableList());
                    metastorePartitions = partitionNameElements.iterator();
                }
            }
            else {
                // no partitions, so data dir is same as table path
                if (shouldSkipMetaStoreForPartition) {
                    tableHandlePartitions = Collections.singletonList("").iterator();
                }
                else {
                    metastorePartitions = Collections.singletonList(Collections.singletonList("")).iterator();
                }
            }

            log.debug(String.format("Finish in %d ms to fetch partition names", timer.endTimer()));
        }

        List<ConnectorSplit> batchHudiSplits = new ArrayList<>();
        int remaining = maxSize;

        log.debug("Target number of splits: " + maxSize);

        // Only process one batch now
        while (remaining > 0 && !isFinished()) {
            if (baseFiles.isEmpty()) {
                HoodieTimer timer1 = new HoodieTimer().startTimer();

                Map<String, List<HivePartitionKey>> batchKeyMap;
                if (shouldSkipMetaStoreForPartition) {
                    List<String> batchTableHandlePartitions = new ArrayList<>();
                    Iterators.limit(tableHandlePartitions, partitionBatchNum).forEachRemaining(batchTableHandlePartitions::add);
                    batchKeyMap = batchTableHandlePartitions.stream().parallel()
                            .map(p -> Pair.of(p, buildPartitionKeys(partitionColumns, buildPartitionValues(p))))
                            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
                    log.debug(">>> shouldSkipMetaStoreForPartition batchKeyMap: %s", batchKeyMap);
                }
                else {
                    List<List<String>> batchMetastorePartitions = new ArrayList<>();
                    Iterators.limit(metastorePartitions, partitionBatchNum).forEachRemaining(batchMetastorePartitions::add);
                    batchKeyMap = batchMetastorePartitions.stream().parallel()
                            .map(partitionNames -> getPartitionPathToKey(
                                    identity, metastore, table, table.getStorage().getLocation(), partitionNames))
                            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
                    log.debug(">>> batchKeyMap: %s", batchKeyMap);
                }
                partitionMap.putAll(batchKeyMap);

                log.debug(String.format("Finish in %d ms to get partition keys: %s", timer1.endTimer(), batchKeyMap));

                timer1 = new HoodieTimer().startTimer();
                List<Pair<HoodieBaseFile, String>> baseFilesToAdd = batchKeyMap.keySet().stream().parallel()
                        .flatMap(relativePartitionPath -> fileSystemView.getLatestBaseFiles(relativePartitionPath)
                                .map(baseFile -> new ImmutablePair<>(baseFile, relativePartitionPath))
                                .collect(Collectors.toList()).stream())
                        .collect(Collectors.toList());

                baseFilesToAdd.forEach(e -> baseFileToPartitionMap.put(e.getKey(), e.getValue()));
                // TODO: skip partitions that are filtered out based on the predicate
                baseFiles.addAll(baseFilesToAdd.stream().map(Pair::getKey).collect(Collectors.toList()));
                log.debug(String.format("Finish in %d ms to get base files", timer1.endTimer()));
            }

            HoodieTimer timer = new HoodieTimer().startTimer();
            List<HoodieBaseFile> batchBaseFiles = new ArrayList<>();
            while (!baseFiles.isEmpty()) {
                HoodieBaseFile baseFile = baseFiles.pollFirst();
                batchBaseFiles.add(baseFile);
            }

            List<HudiSplit> hudiSplitsToAdd = batchBaseFiles.stream().parallel()
                    .flatMap(baseFile -> {
                        List<HudiSplit> hudiSplits;
                        try {
                            hudiSplits = getSplits(
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
                                                    partitionMap.get(baseFileToPartitionMap.get(baseFile))));
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
            batchHudiSplits.addAll(hudiSplitsToAdd);
            remaining -= hudiSplitsToAdd.size();
            log.debug(String.format("Finish in %d ms to get batch splits", timer.endTimer()));
            if (remaining < hudiSplitsToAdd.size()) {
                break;
            }
        }

        log.info("Number of Hudi splits generated in the batch: " + batchHudiSplits.size());

        return batchHudiSplits;
    }

    private Pair<String, List<HivePartitionKey>> getPartitionPathToKey(
            HiveIdentity identity,
            HiveMetastore metastore,
            Table table,
            String tablePath,
            List<String> partitionValues)
    {
        log.debug(">>> Inside getPartitionPathToKey: %s", partitionValues);
        Optional<Partition> partition1;
        String relativePartitionPath;
        List<HivePartitionKey> partitionKeys;
        partition1 = metastore.getPartition(identity, table, partitionValues);
        partition1.ifPresent(p -> log.debug("Partition from metastore: %s", p));
        String dataDir1 = partition1.isPresent()
                ? partition1.get().getStorage().getLocation()
                : tablePath;
        log.debug(">>> basePath: %s,  dataDir1: %s", tablePath, dataDir1);
        relativePartitionPath = FSUtils.getRelativePartitionPath(new Path(tablePath), new Path(dataDir1));
        log.debug(">>> relativePartitionPath: %s", relativePartitionPath);
        partitionKeys = getPartitionKeys(table, partition1);
        log.debug(">>> Partition keys: %s", partitionKeys);
        return new ImmutablePair<>(relativePartitionPath, partitionKeys);
    }
}
