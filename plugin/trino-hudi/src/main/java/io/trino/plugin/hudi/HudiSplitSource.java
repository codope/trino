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
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartition;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreUtil;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.spi.connector.ConnectorPartitionHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
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
import static java.util.stream.Collectors.toList;
import static org.apache.hudi.common.fs.FSUtils.getRelativePartitionPath;

public class HudiSplitSource
        implements ConnectorSplitSource
{
    private static final Logger LOG = Logger.get(HudiSplitSource.class);
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
    private final boolean shouldSkipMetastoreForPartition;
    private final Map<HoodieBaseFile, String> baseFileToPartitionMap;
    private final List<HiveColumnHandle> partitionColumnHandles;
    private final ArrayDeque<HoodieBaseFile> baseFiles = new ArrayDeque<>();
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
            List<HiveColumnHandle> partitionColumnHandles,
            boolean metadataEnabled,
            boolean shouldSkipMetastoreForPartition)
    {
        this.identity = identity;
        this.metastore = metastore;
        this.tableName = tableHandle.getSchemaTableName();
        this.table = metastore.getTable(identity, tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));
        this.conf = requireNonNull(conf, "conf is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.partitionMap = new HashMap<>();
        this.partitionColumnHandles = partitionColumnHandles;
        this.metadataEnabled = metadataEnabled;
        this.shouldSkipMetastoreForPartition = shouldSkipMetastoreForPartition;
        this.metaClient = tableHandle.getMetaClient().orElseGet(() -> getMetaClient(conf, tableHandle.getBasePath()));
        this.fileSystem = metaClient.getFs();
        this.baseFileToPartitionMap = new HashMap<>();
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
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
        if (shouldSkipMetastoreForPartition) {
            return !tableHandlePartitions.hasNext() && baseFiles.isEmpty();
        }
        return !metastorePartitions.hasNext() && baseFiles.isEmpty();
    }

    private List<ConnectorSplit> getSplitsForSnapshotMode(int maxSize)
            throws IOException
    {
        if (isNull(this.fileSystemView)) {
            // First time calling this
            // Load the timeline and file status only once
            HoodieEngineContext engineContext = new HoodieLocalEngineContext(conf);
            HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                    .enable(metadataEnabled)
                    .build();
            // Scan the file system to load the instants from timeline
            this.fileSystemView = FileSystemViewManager.createInMemoryFileSystemView(engineContext, metaClient, metadataConfig);
            partitionColumns = table.getPartitionColumns();

            if (!partitionColumns.isEmpty()) {
                HoodieTimer timer = new HoodieTimer().startTimer();

                Optional<List<HivePartition>> partitions = tableHandle.getPartitions();
                if (partitions.isEmpty() || partitions.get().isEmpty()) {
                    throw new HoodieIOException("Cannot find partitions in the table");
                }
                List<Type> partitionTypes = partitionColumnHandles.stream()
                        .map(HiveColumnHandle::getType)
                        .collect(toList());

                LOG.warn("partitionColumnHandles: " + partitionColumnHandles);

                TupleDomain<String> partitionKeyFilters = MetastoreUtil.computePartitionKeyFilter(
                        partitionColumnHandles, tableHandle.getPredicate());
                LOG.warn("shouldSkipMetastoreForPartition: " + shouldSkipMetastoreForPartition);
                LOG.warn("Table handle predicate : " + tableHandle.getPredicate().toString());

                if (shouldSkipMetastoreForPartition) {
                    List<String> tablePartitions = tableHandle.getPartitions().orElseGet(ImmutableList::of).stream()
                            .map(HivePartition::getPartitionId)
                            .map(partitionName -> HudiUtil.parseValuesAndFilterPartition(
                                    tableName, partitionName, partitionColumnHandles, partitionTypes, tableHandle.getPredicate()))
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .collect(toList());
                    LOG.warn("full partition names after filtering: " + tablePartitions);
                    tableHandlePartitions = tablePartitions.iterator();
                }
                else {
                    List<String> fullPartitionNames = metastore.getPartitionNamesByFilter(
                                    identity,
                                    tableName.getSchemaName(),
                                    tableName.getTableName(),
                                    partitionColumns.stream()
                                            .map(Column::getName)
                                            .collect(Collectors.toList()),
                                    partitionKeyFilters)
                            .orElseThrow(() -> new TableNotFoundException(tableHandle.getSchemaTableName()))
                            .stream()
                            // Apply extra filters which may not be done by getPartitionNamesByFilter
                            .map(partitionName -> HudiUtil.parseValuesAndFilterPartition(
                                    tableName, partitionName, partitionColumnHandles, partitionTypes, tableHandle.getPredicate()))
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .collect(toList());
                    LOG.warn("full partition names after filtering: " + fullPartitionNames);
                    List<List<String>> partitionNameElements = fullPartitionNames
                            .stream()
                            .map(HiveUtil::toPartitionValues)
                            .collect(toImmutableList());
                    metastorePartitions = partitionNameElements.iterator();
                }
                LOG.warn(String.format("Time to filter partitions: %d ms", timer.endTimer()));
            }
            else {
                if (shouldSkipMetastoreForPartition) {
                    tableHandlePartitions = Collections.singletonList("").iterator();
                }
                else {
                    metastorePartitions = Collections.singletonList(Collections.singletonList("")).iterator();
                }
            }
        }

        List<ConnectorSplit> batchHudiSplits = new ArrayList<>();
        int remaining = maxSize;

        while (remaining > 0 && !isFinished()) {
            if (baseFiles.isEmpty()) {
                Map<String, List<HivePartitionKey>> batchKeyMap;
                if (shouldSkipMetastoreForPartition) {
                    List<String> batchTableHandlePartitions = new ArrayList<>();
                    Iterators.limit(tableHandlePartitions, partitionBatchNum).forEachRemaining(batchTableHandlePartitions::add);
                    batchKeyMap = batchTableHandlePartitions.stream().parallel()
                            .map(p -> Pair.of(p, buildPartitionKeys(partitionColumns, buildPartitionValues(p))))
                            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
                }
                else {
                    List<List<String>> batchMetastorePartitions = new ArrayList<>();
                    Iterators.limit(metastorePartitions, partitionBatchNum).forEachRemaining(batchMetastorePartitions::add);
                    batchKeyMap = batchMetastorePartitions.stream().parallel()
                            .map(partitionNames -> getPartitionPathToKeyUsingMetastore(
                                    identity, metastore, table, table.getStorage().getLocation(), partitionNames))
                            .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
                }

                partitionMap.putAll(batchKeyMap);
                List<Pair<HoodieBaseFile, String>> baseFilesToAdd = batchKeyMap.keySet().stream().parallel()
                        .flatMap(relativePartitionPath -> fileSystemView.getLatestBaseFiles(relativePartitionPath)
                                .map(baseFile -> new ImmutablePair<>(baseFile, relativePartitionPath))
                                .collect(Collectors.toList()).stream())
                        .collect(Collectors.toList());

                baseFilesToAdd.forEach(e -> baseFileToPartitionMap.put(e.getKey(), e.getValue()));
                // TODO: skip partitions that are filtered out based on the predicate
                baseFiles.addAll(baseFilesToAdd.stream().map(Pair::getKey).collect(Collectors.toList()));
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
            if (remaining < hudiSplitsToAdd.size()) {
                break;
            }
        }

        return batchHudiSplits;
    }

    private Pair<String, List<HivePartitionKey>> getPartitionPathToKeyUsingMetastore(
            HiveIdentity identity,
            HiveMetastore metastore,
            Table table,
            String tablePath,
            List<String> partitionValues)
    {
        Optional<Partition> partition;
        String relativePartitionPath;
        List<HivePartitionKey> partitionKeys;
        partition = metastore.getPartition(identity, table, partitionValues);
        String dataDir1 = partition.isPresent()
                ? partition.get().getStorage().getLocation()
                : tablePath;
        relativePartitionPath = getRelativePartitionPath(new Path(tablePath), new Path(dataDir1));
        partitionKeys = getPartitionKeys(table, partition);

        return new ImmutablePair<>(relativePartitionPath, partitionKeys);
    }
}
