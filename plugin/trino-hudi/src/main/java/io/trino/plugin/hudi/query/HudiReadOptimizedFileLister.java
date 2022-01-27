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

package io.trino.plugin.hudi.query;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreUtil;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.HudiUtil;
import io.trino.plugin.hudi.partition.HiveHudiPartitionInfo;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;
import org.apache.hudi.hive.NonPartitionedExtractor;
import org.apache.hudi.hive.PartitionValueExtractor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.plugin.hudi.partition.HudiPartitionInfoFactory.buildHudiPartitionInfo;
import static java.lang.String.format;
import static java.util.Objects.isNull;

public class HudiReadOptimizedFileLister
        implements HudiFileLister
{
    private static final Logger log = Logger.get(HudiReadOptimizedFileLister.class);

    final HoodieMetadataConfig metadataConfig;
    final HoodieEngineContext engineContext;
    final HoodieTableMetaClient metaClient;
    final HudiTableHandle tableHandle;
    final HiveMetastore hiveMetastore;
    final Table hiveTable;
    final SchemaTableName tableName;
    final List<HiveColumnHandle> partitionColumnHandles;
    final boolean shouldSkipMetastoreForPartition;
    final HoodieTableFileSystemView fileSystemView;
    final TupleDomain<String> partitionKeysFilter;
    final List<Column> partitionColumns;

    private List<String> hivePartitionNames;

    public HudiReadOptimizedFileLister(
            HoodieMetadataConfig metadataConfig,
            HoodieEngineContext engineContext,
            HudiTableHandle tableHandle,
            HoodieTableMetaClient metaClient,
            HiveMetastore hiveMetastore,
            Table hiveTable,
            List<HiveColumnHandle> partitionColumnHandles,
            boolean shouldSkipMetastoreForPartition)
    {
        this.metadataConfig = metadataConfig;
        this.engineContext = engineContext;
        this.tableHandle = tableHandle;
        this.tableName = tableHandle.getSchemaTableName();
        this.metaClient = metaClient;
        this.hiveMetastore = hiveMetastore;
        this.hiveTable = hiveTable;
        this.partitionColumnHandles = partitionColumnHandles;
        this.shouldSkipMetastoreForPartition = shouldSkipMetastoreForPartition;
        this.fileSystemView = FileSystemViewManager.createInMemoryFileSystemView(engineContext, metaClient, metadataConfig);
        this.partitionKeysFilter = MetastoreUtil.computePartitionKeyFilter(partitionColumnHandles, tableHandle.getPartitionPredicates());
        this.partitionColumns = hiveTable.getPartitionColumns();
    }

    @Override
    public List<HudiPartitionInfo> getPartitionsToScan()
    {
        HoodieTimer timer = new HoodieTimer().startTimer();

        List<HudiPartitionInfo> allPartitionInfoList = null;

        if (shouldSkipMetastoreForPartition) {
            try {
                // Use relative partition path and other context to construct
                // InternalHudiPartitionInfo instances
                PartitionValueExtractor partitionValueExtractor = partitionColumns.isEmpty()
                        ? new NonPartitionedExtractor()
                        : inferPartitionValueExtractorWithHiveMetastore();
                List<String> relativePartitionPathList = partitionColumns.isEmpty()
                        ? Collections.singletonList("")
                        : TimelineUtils.getPartitionsWritten(metaClient.getActiveTimeline());
                allPartitionInfoList = relativePartitionPathList.stream()
                        .map(relativePartitionPath ->
                                buildHudiPartitionInfo(
                                        shouldSkipMetastoreForPartition,
                                        Option.of(relativePartitionPath),
                                        Option.empty(),
                                        Option.of(partitionValueExtractor),
                                        partitionColumns,
                                        partitionColumnHandles,
                                        tableHandle.getPartitionPredicates(),
                                        hiveTable,
                                        hiveMetastore))
                        .collect(Collectors.toList());
            }
            catch (HoodieIOException e) {
                log.warn("Cannot skip Hive Metastore for scanning partitions. Falling back to using Hive Metastore.");
            }
        }

        if (isNull(allPartitionInfoList)) {
            // Use Hive partition names and other context to construct
            // HiveHudiPartitionInfo instances
            if (isNull(hivePartitionNames)) {
                hivePartitionNames = partitionColumns.isEmpty()
                        ? Collections.singletonList("")
                        : getPartitionNamesFromHiveMetastore(partitionKeysFilter);
            }

            allPartitionInfoList = hivePartitionNames.stream()
                    .map(hivePartitionName ->
                            buildHudiPartitionInfo(shouldSkipMetastoreForPartition,
                                    Option.empty(),
                                    Option.of(hivePartitionName),
                                    Option.empty(),
                                    partitionColumns,
                                    partitionColumnHandles,
                                    tableHandle.getPartitionPredicates(),
                                    hiveTable,
                                    hiveMetastore))
                    .collect(Collectors.toList());
        }

        List<HudiPartitionInfo> filteredPartitionInfoList = allPartitionInfoList.stream()
                .filter(partitionInfo -> partitionInfo.getHivePartitionKeys().isEmpty() || partitionInfo.doesMatchPredicates())
                .collect(Collectors.toList());

        log.debug(format(
                "Get partitions to scan in %d ms (shouldSkipMetastoreForPartition: %s): %s",
                timer.endTimer(),
                shouldSkipMetastoreForPartition,
                filteredPartitionInfoList));

        return filteredPartitionInfoList;
    }

    @Override
    public List<FileStatus> listStatus(HudiPartitionInfo partitionInfo)
    {
        return fileSystemView.getLatestBaseFiles(partitionInfo.getRelativePartitionPath())
                .map(baseFile -> {
                    try {
                        return HoodieInputFormatUtils.getFileStatus(baseFile);
                    }
                    catch (IOException e) {
                        throw new HoodieIOException("Error getting file status of " + baseFile.getPath(), e);
                    }
                })
                .collect(Collectors.toList());
    }

    @VisibleForTesting
    PartitionValueExtractor inferPartitionValueExtractorWithHiveMetastore()
            throws HoodieIOException
    {
        hivePartitionNames = getPartitionNamesFromHiveMetastore(TupleDomain.all());
        if (hivePartitionNames.isEmpty()) {
            throw new HoodieIOException("Cannot infer partition value extractor with Hive Metastore: partition list is empty!");
        }
        HiveHudiPartitionInfo partitionHiveInfo = new HiveHudiPartitionInfo(
                hivePartitionNames.get(0),
                partitionColumns,
                partitionColumnHandles,
                tableHandle.getPartitionPredicates(),
                hiveTable,
                hiveMetastore);
        String relativePartitionPath = partitionHiveInfo.getRelativePartitionPath();
        List<String> partitionValues = partitionHiveInfo.getHivePartitionKeys().stream()
                .map(HivePartitionKey::getValue).collect(Collectors.toList());
        return HudiUtil.inferPartitionValueExtractor(relativePartitionPath, partitionValues);
    }

    private List<String> getPartitionNamesFromHiveMetastore(TupleDomain<String> partitionKeysFilter)
    {
        return hiveMetastore.getPartitionNamesByFilter(
                tableName.getSchemaName(),
                tableName.getTableName(),
                partitionColumns.stream().map(Column::getName).collect(Collectors.toList()),
                partitionKeysFilter).orElseThrow(() -> new TableNotFoundException(tableHandle.getSchemaTableName()));
    }

    @Override
    public Map<String, Optional<Partition>> getPartitions(List<String> partitionNames)
    {
        return hiveMetastore.getPartitionsByNames(hiveTable, partitionNames);
    }

    @Override
    public void close()
    {
        if (!isNull(fileSystemView) && !fileSystemView.isClosed()) {
            fileSystemView.close();
        }
    }
}
