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
import static io.trino.plugin.hudi.HudiUtil.getMetaClient;
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
    private List<String> partitionColumnNames;
    private Iterator<List<String>> partitionNames;

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
        fileSystemView.close();
    }

    @Override
    public boolean isFinished()
    {
        return !partitionNames.hasNext() && baseFiles.isEmpty();
    }

    private List<ConnectorSplit> getSplitsForSnapshotMode(int maxSize) throws IOException
    {
        if (this.fileSystemView == null) {
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
            log.warn(String.format("Finish in %d ms to load table view", timer.endTimer()));

            timer = new HoodieTimer().startTimer();
            partitionColumnNames = table.getPartitionColumns().stream()
                    .map(Column::getName)
                    .collect(toImmutableList());
            log.warn("Column Names: " + partitionColumnNames);
            List<String> fullPartitionNames = new ArrayList<>();
            List<List<String>> partitionNameElements = new ArrayList<>();
            if (!partitionColumnNames.isEmpty()) {
                fullPartitionNames = metastore.getPartitionNamesByFilter(identity, tableName.getSchemaName(), tableName.getTableName(), partitionColumnNames, TupleDomain.all())
                        .orElseThrow(() -> new TableNotFoundException(tableHandle.getSchemaTableName()));
                partitionNameElements = fullPartitionNames
                        .stream()
                        .map(HiveUtil::toPartitionValues)
                        .collect(toImmutableList());
                partitionNames = partitionNameElements.iterator();
            }
            else {
                // no partitions, so data dir is same as table path
                partitionNames = Collections.singletonList(Collections.singletonList("")).iterator();
            }

            log.warn(String.format("Finish in %d ms. Partition Names: %s", timer.endTimer(), fullPartitionNames));
            log.warn(String.format("Partition Name elements: %s", partitionNameElements));
        }

        List<ConnectorSplit> batchHudiSplits = new ArrayList<>();
        int remaining = maxSize;

        log.debug("Target number of splits: " + maxSize);

        // Only process one batch now
        while (remaining > 0 && !isFinished()) {
            if (baseFiles.isEmpty()) {
                HoodieTimer timer1 = new HoodieTimer().startTimer();

                List<List<String>> batchPartitionNames = new ArrayList<>();
                Iterators.limit(partitionNames, partitionBatchNum).forEachRemaining(batchPartitionNames::add);

                Map<String, List<HivePartitionKey>> batchKeyMap =
                        batchPartitionNames.stream().parallel()
                                .map(partitionNames -> getPartitionPathToKey(
                                        identity, metastore, table, table.getStorage().getLocation(), partitionColumnNames, partitionNames))
                                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
                partitionMap.putAll(batchKeyMap);

                log.warn(String.format("Finish in %d ms to get partition keys: %s", timer1.endTimer(), batchKeyMap.toString()));

                timer1 = new HoodieTimer().startTimer();
                List<Pair<HoodieBaseFile, String>> baseFilesToAdd = batchKeyMap.keySet().stream().parallel()
                        .flatMap(relativePartitionPath -> fileSystemView.getLatestBaseFiles(relativePartitionPath)
                                .map(baseFile -> new ImmutablePair<>(baseFile, relativePartitionPath))
                                .collect(Collectors.toList()).stream())
                        .collect(Collectors.toList());

                baseFilesToAdd.forEach(e -> baseFileToPartitionMap.put(e.getKey(), e.getValue()));
                // TODO: skip partitions that are filtered out based on the predicate
                baseFiles.addAll(baseFilesToAdd.stream().map(Pair::getKey).collect(Collectors.toList()));
                log.warn(String.format("Finish in %d ms to get base files", timer1.endTimer()));
            }

            HoodieTimer timer = new HoodieTimer().startTimer();
            List<HoodieBaseFile> batchBaseFiles = new ArrayList<>();
            while (!baseFiles.isEmpty()) {
                HoodieBaseFile baseFile = baseFiles.pollFirst();
                batchBaseFiles.add(baseFile);
            }

            List<HudiSplit> hudiSplitsToAdd = batchBaseFiles.stream().parallel()
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
            log.warn(String.format("Finish in %d ms to get batch splits", timer.endTimer()));
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
            List<String> columnNames,
            List<String> partitionName)
    {
        Optional<Partition> partition1 = Optional.empty();
        String relativePartitionPath = "";
        List<HivePartitionKey> partitionKeys = new ArrayList<>();
        if (!columnNames.isEmpty()) {
            if (shouldSkipMetaStoreForPartition) {
                StringBuilder partitionPathBuilder = new StringBuilder();
                for (int i = 0; i < columnNames.size(); i++) {
                    if (partitionPathBuilder.length() > 0) {
                        partitionPathBuilder.append("/");
                    }
                    String columnName = columnNames.get(i);
                    String value = partitionName.get(i);
                    partitionKeys.add(new HivePartitionKey(columnName, value));
                    partitionPathBuilder.append(columnName);
                    partitionPathBuilder.append("=");
                    partitionPathBuilder.append(value);
                }
                if (columnNames.size() == 1) {
                    String value = partitionName.get(0);
                    if (value.contains("-")) {
                        relativePartitionPath = value.replace("-", "/");
                    }
                    else {
                        relativePartitionPath = partitionPathBuilder.toString();
                    }
                }
                else {
                    relativePartitionPath = partitionPathBuilder.toString();
                }
            }
            else {
                partition1 = metastore.getPartition(identity, table, partitionName);
                String dataDir1 = partition1.isPresent()
                        ? partition1.get().getStorage().getLocation()
                        : tablePath;
                log.warn(">>> basePath: %s,  dataDir1: %s", tablePath, dataDir1);
                relativePartitionPath = FSUtils.getRelativePartitionPath(new Path(tablePath), new Path(dataDir1));
                partitionKeys = getPartitionKeys(table, partition1);
            }
        }
        return new ImmutablePair<>(relativePartitionPath, partitionKeys);
    }
}
