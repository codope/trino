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
import io.trino.spi.connector.ConnectorPartitionHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static com.google.common.collect.Iterators.limit;
import static io.trino.plugin.hudi.HudiUtil.getMetaClient;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class HudiSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(HudiSplitSource.class);

    private final HudiTableHandle tableHandle;
    private final HoodieTableMetaClient metaClient;
    private final HoodieTableFileSystemView fileSystemView;
    private final Iterator<HoodieBaseFile> hoodieBaseFileIterator;
    private final List<HivePartitionKey> partitionKeys;
    private final boolean metadataEnabled;
    private final Optional<FileStatus[]> fileStatuses;
    private final String tablePath;

    public HudiSplitSource(
            HudiTableHandle tableHandle,
            Configuration conf,
            List<HivePartitionKey> partitionKeys,
            boolean metadataEnabled,
            Optional<FileStatus[]> fileStatuses,
            String tablePath,
            Map<HivePartitionKey, Path> dataDir)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.partitionKeys = requireNonNull(partitionKeys, "partitionKeys is null");
        this.metadataEnabled = metadataEnabled;
        this.fileStatuses = fileStatuses;
        this.tablePath = tablePath;
        this.metaClient = tableHandle.getMetaClient().orElseGet(() -> getMetaClient(conf, tableHandle.getBasePath()));
        HoodieEngineContext engineContext = new HoodieLocalEngineContext(conf);
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                .enable(metadataEnabled)
                .build();
        this.fileSystemView = FileSystemViewManager.createInMemoryFileSystemView(engineContext, metaClient, metadataConfig);

        if (partitionKeys.isEmpty()) {
            String partition = FSUtils.getRelativePartitionPath(new Path(tablePath), new Path(tablePath));
            this.hoodieBaseFileIterator = fileSystemView.getLatestBaseFiles(partition).iterator();
        }
        else {
            ImmutableList.Builder<HoodieBaseFile> baseFiles = ImmutableList.builder();
            fileSystemView.addFilesToView(fileStatuses.orElseGet(() -> new FileStatus[0]));
            for (HivePartitionKey partitionKey : partitionKeys) {
                log.warn("Path: %s,  Partition: %s", dataDir.get(partitionKey), partitionKey);
                String partition = FSUtils.getRelativePartitionPath(new Path(tablePath), dataDir.get(partitionKey));
                log.warn("Partition: %s", partition);
                if (fileStatuses.isPresent()) {
                    log.warn(">>> FileStatus present adding to view: %s", fileStatuses.get().length);
                    baseFiles.addAll(fileSystemView.fetchLatestBaseFiles(partition).collect(Collectors.toList()));
                }
                else {
                    baseFiles.addAll(fileSystemView.getLatestBaseFiles(partition).collect(Collectors.toList()));
                }
            }
            this.hoodieBaseFileIterator = baseFiles.build().iterator();
        }
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        log.debug("Getting next batch with partitionKeys: " + partitionKeys);
        List<ConnectorSplit> splits = new ArrayList<>();
        Iterator<HoodieBaseFile> baseFileIterator = limit(hoodieBaseFileIterator, maxSize);
        while (baseFileIterator.hasNext()) {
            HoodieBaseFile baseFile = baseFileIterator.next();
            log.warn(">>>> Base File: " + baseFile);
            FileStatus fileStatus = baseFile.getFileStatus();
            log.warn(">>>> FileStatus: " + fileStatus.toString());
            /*String[] name = new String[] {"localhost:" + DFS_DATANODE_DEFAULT_PORT};
            String[] host = new String[] {"localhost"};
            BlockLocation[] blockLocations = new BlockLocation[] {new BlockLocation(name, host, 0L, fileStatus.getLen())};*/
            splits.add(new HudiSplit(
                    baseFile.getPath(),
                    0L,
                    baseFile.getFileLen(),
                    baseFile.getFileSize(),
                    ImmutableList.of(),
                    tableHandle.getPredicate(),
                    partitionKeys));
        }

        return completedFuture(new ConnectorSplitBatch(splits, isFinished()));
    }

    @Override
    public void close()
    {
        // TODO: close file iterable
    }

    @Override
    public boolean isFinished()
    {
        return !hoodieBaseFileIterator.hasNext();
    }
}
