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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.exception.HoodieIOException;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static io.trino.plugin.hudi.HudiUtil.getMetaClient;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class HudiSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(HudiSplitSource.class);
    private final Configuration conf;
    private final HudiTableHandle tableHandle;
    private final FileSystem fileSystem;
    private final HoodieTableMetaClient metaClient;
    private final DataSize maxSplitSize;
    private final List<HivePartitionKey> partitionKeys;
    private final boolean metadataEnabled;
    private final String tablePath;
    private HoodieTableFileSystemView fileSystemView;
    private ArrayDeque<HoodieBaseFile> baseFiles;
    private Iterator<String> relativePartitionPaths;

    public HudiSplitSource(
            ConnectorSession session,
            HudiTableHandle tableHandle,
            Configuration conf,
            List<HivePartitionKey> partitionKeys,
            Iterator<String> relativePartitionPaths,
            boolean metadataEnabled,
            String tablePath)
    {
        requireNonNull(session, "session is null");
        this.conf = requireNonNull(conf, "conf is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.partitionKeys = requireNonNull(partitionKeys, "partitionKeys is null");
        this.relativePartitionPaths = requireNonNull(relativePartitionPaths, "relativePartitionPaths is null");
        this.metadataEnabled = metadataEnabled;
        this.tablePath = tablePath;
        this.metaClient = tableHandle.getMetaClient().orElseGet(() -> getMetaClient(conf, tableHandle.getBasePath()));
        this.fileSystem = metaClient.getFs();
        log.debug("Table path: %s \nDirectory: %s", tablePath);
        this.maxSplitSize = DataSize.ofBytes(32 * 1024 * 1024);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        log.debug("Getting next batch with partitionKeys: " + partitionKeys);
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
        // TODO: close file iterable
    }

    @Override
    public boolean isFinished()
    {
        return !relativePartitionPaths.hasNext() && baseFiles.isEmpty();
    }

    private List<ConnectorSplit> getSplitsForSnapshotMode(int maxSize) throws IOException
    {
        if (this.fileSystemView == null) {
            // First time calling this
            // Load the timeline and file status only once
            HoodieEngineContext engineContext = new HoodieLocalEngineContext(conf);
            HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                    .enable(metadataEnabled)
                    .build();
            // Scan the file system to load the instants from timeline
            log.debug("Loading file system view for " + metaClient.getBasePath());
            this.fileSystemView = FileSystemViewManager.createInMemoryFileSystemView(engineContext, metaClient, metadataConfig);
            this.baseFiles = new ArrayDeque<>();
        }

        List<ConnectorSplit> batchHudiSplits = new ArrayList<>();
        int remaining = maxSize;

        log.debug("Target number of splits: " + maxSize);

        while (remaining > 0 && !isFinished()) {
            if (baseFiles.isEmpty()) {
                if (relativePartitionPaths.hasNext()) {
                    String relativePartitionPath = relativePartitionPaths.next();
                    // TODO: skip partitions that are filtered out based on the predicate
                    baseFiles.addAll(fileSystemView.getLatestBaseFiles(relativePartitionPath)
                            .collect(Collectors.toList()));
                }
            }

            while (remaining > 0 && !baseFiles.isEmpty()) {
                HoodieBaseFile baseFile = baseFiles.pollFirst();
                log.debug(String.format("Remaining: %d base file: %s", remaining, baseFile.getPath()));
                List<FileSplit> fileSplits = HudiUtil.getSplits(fileSystem, baseFile.getFileStatus());
                fileSplits.forEach(fileSplit -> {
                    try {
                        log.debug(String.format(">>>> File split: %s start=%d len=%d",
                                fileSplit.getPath(), fileSplit.getStart(), fileSplit.getLength()));
                        batchHudiSplits.add(new HudiSplit(
                                fileSplit.getPath().toString(),
                                fileSplit.getStart(),
                                fileSplit.getLength(),
                                metaClient.getFs().getLength(fileSplit.getPath()),
                                ImmutableList.of(),
                                tableHandle.getPredicate(),
                                partitionKeys));
                    }
                    catch (IOException e) {
                        throw new HoodieIOException("Unable to add splits for " + fileSplit.getPath().toString(), e);
                    }
                });
                remaining -= fileSplits.size();
            }
        }

        log.info("Number of Hudi splits generated in the batch: " + batchHudiSplits.size());

        return batchHudiSplits;
    }
}
