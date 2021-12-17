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
import io.airlift.units.DataSize;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.spi.connector.ConnectorPartitionHandle;
import io.trino.spi.connector.ConnectorSession;
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
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.google.common.collect.Iterators.limit;
import static io.trino.plugin.hudi.HudiUtil.getMetaClient;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class HudiSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(HudiSplitSource.class);
    private final Configuration conf;
    private final HudiTableHandle tableHandle;
    private final HoodieTableMetaClient metaClient;
    private final HoodieTableFileSystemView fileSystemView;
    private final Iterator<HoodieBaseFile> hoodieBaseFileIterator;
    private final DataSize maxSplitSize;
    private final List<HivePartitionKey> partitionKeys;
    private final boolean metadataEnabled;
    private final Optional<FileStatus[]> fileStatuses;
    private final String tablePath;
    private final String dataDir;

    public HudiSplitSource(
            ConnectorSession session,
            HudiTableHandle tableHandle,
            Configuration conf,
            List<HivePartitionKey> partitionKeys,
            boolean metadataEnabled,
            Optional<FileStatus[]> fileStatuses,
            String tablePath,
            String dataDir)
    {
        requireNonNull(session, "session is null");
        this.conf = requireNonNull(conf, "conf is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.partitionKeys = requireNonNull(partitionKeys, "partitionKeys is null");
        this.metadataEnabled = metadataEnabled;
        this.fileStatuses = fileStatuses;
        this.tablePath = tablePath;
        this.dataDir = dataDir;
        this.metaClient = tableHandle.getMetaClient().orElseGet(() -> getMetaClient(conf, tableHandle.getBasePath()));
        HoodieEngineContext engineContext = new HoodieLocalEngineContext(conf);
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                .enable(metadataEnabled)
                .build();
        this.fileSystemView = FileSystemViewManager.createInMemoryFileSystemView(engineContext, metaClient, metadataConfig);

        log.debug("Table path: %s \nDirectory: %s", tablePath, dataDir);
        String partition = FSUtils.getRelativePartitionPath(new Path(tablePath), new Path(dataDir));
        log.debug("Partition: %s", partition);
        if (fileStatuses.isPresent()) {
            log.warn(">>> FileStatus present adding to view: %s", fileStatuses.get().length);
            fileSystemView.addFilesToView(fileStatuses.get());
            this.hoodieBaseFileIterator = fileSystemView.fetchLatestBaseFiles(partition).iterator();
        }
        else {
            this.hoodieBaseFileIterator = fileSystemView.getLatestBaseFiles(partition).iterator();
        }
        this.maxSplitSize = DataSize.ofBytes(32 * 1024 * 1024);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        log.debug("Getting next batch with partitionKeys: " + partitionKeys);
        List<ConnectorSplit> splits = new ArrayList<>();
        long maxSplitBytes = maxSplitSize.toBytes();
        Iterator<HoodieBaseFile> baseFileIterator = limit(hoodieBaseFileIterator, maxSize);
        while (baseFileIterator.hasNext()) {
            HoodieBaseFile baseFile = baseFileIterator.next();
            log.warn(">>>> Base File: " + baseFile);
            FileStatus fileStatus = baseFile.getFileStatus();
            log.warn(">>>> FileStatus: " + fileStatus.toString());
            /*String[] name = new String[] {"localhost:" + DFS_DATANODE_DEFAULT_PORT};
            String[] host = new String[] {"localhost"};
            BlockLocation[] blockLocations = new BlockLocation[] {new BlockLocation(name, host, 0L, fileStatus.getLen())};*/
            long remainingFileBytes = baseFile.getFileSize();
            long start = 0;
            long splitBytes;
            log.debug(String.format("Base file %s, %d bytes", baseFile.getPath(), remainingFileBytes));
            while (remainingFileBytes > 0) {
                if (remainingFileBytes <= maxSplitBytes) {
                    splitBytes = remainingFileBytes;
                }
                else if (maxSplitBytes * 2 >= remainingFileBytes) {
                    splitBytes = remainingFileBytes / 2;
                }
                else {
                    splitBytes = maxSplitBytes;
                }
                log.debug(String.format("Split: start=%d len=%d remaining=%d", start, splitBytes, remainingFileBytes));
                splits.add(new HudiSplit(
                        baseFile.getPath(),
                        start,
                        splitBytes,
                        baseFile.getFileSize(),
                        ImmutableList.of(),
                        tableHandle.getPredicate(),
                        partitionKeys));
                start += splitBytes;
                remainingFileBytes -= splitBytes;
            }
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
