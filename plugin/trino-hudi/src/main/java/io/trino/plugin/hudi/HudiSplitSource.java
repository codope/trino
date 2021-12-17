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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
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
    private final Iterator<InputSplit> inputSplitIterator;
    private final DataSize maxSplitSize;
    private final List<HivePartitionKey> partitionKeys;
    private final boolean metadataEnabled;
    private final String tablePath;
    private final String dataDir;

    public HudiSplitSource(
            ConnectorSession session,
            HudiTableHandle tableHandle,
            Configuration conf,
            List<HivePartitionKey> partitionKeys,
            boolean metadataEnabled,
            InputSplit[] inputSplits,
            String tablePath,
            String dataDir)
    {
        requireNonNull(session, "session is null");
        this.conf = requireNonNull(conf, "conf is null");
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.partitionKeys = requireNonNull(partitionKeys, "partitionKeys is null");
        this.metadataEnabled = metadataEnabled;
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
        this.inputSplitIterator = Arrays.stream(inputSplits).iterator();
        this.maxSplitSize = DataSize.ofBytes(32 * 1024 * 1024);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        log.debug("Getting next batch with partitionKeys: " + partitionKeys);
        List<ConnectorSplit> connectorSplits = new ArrayList<>();
        Iterator<InputSplit> batchSplitIterator = limit(inputSplitIterator, maxSize);
        while (batchSplitIterator.hasNext()) {
            FileSplit fileSplit = (FileSplit) batchSplitIterator.next();
            log.debug(String.format(">>>> File split: %s start=%d len=%d",
                    fileSplit.getPath(), fileSplit.getStart(), fileSplit.getLength()));
            try {
                connectorSplits.add(new HudiSplit(
                        fileSplit.getPath().toString(),
                        fileSplit.getStart(),
                        fileSplit.getLength(),
                        metaClient.getFs().getLength(fileSplit.getPath()),
                        ImmutableList.of(),
                        tableHandle.getPredicate(),
                        partitionKeys));
            }
            catch (IOException e) {
                log.error("Error getting file size: " + e.getMessage());
            }
        }

        return completedFuture(new ConnectorSplitBatch(connectorSplits, isFinished()));
    }

    @Override
    public void close()
    {
        // TODO: close file iterable
    }

    @Override
    public boolean isFinished()
    {
        return !inputSplitIterator.hasNext();
    }
}
