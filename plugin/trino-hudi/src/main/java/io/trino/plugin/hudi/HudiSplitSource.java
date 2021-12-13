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
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.spi.connector.ConnectorPartitionHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;

import java.util.ArrayList;
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
    private final HudiTableHandle tableHandle;
    private final HoodieTableMetaClient metaClient;
    private final HoodieTableFileSystemView fileSystemView;
    private final Iterator<HoodieBaseFile> hoodieBaseFileIterator;

    public HudiSplitSource(HudiTableHandle tableHandle, Configuration conf)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.metaClient = tableHandle.getMetaClient().orElseGet(() -> getMetaClient(conf, tableHandle.getBasePath()));
        HoodieEngineContext engineContext = new HoodieLocalEngineContext(conf);
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                .enable(false)
                .build();
        this.fileSystemView = FileSystemViewManager.createInMemoryFileSystemView(engineContext, metaClient, metadataConfig);
        this.hoodieBaseFileIterator = fileSystemView.getLatestBaseFiles().iterator();
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        List<ConnectorSplit> splits = new ArrayList<>();
        Iterator<HoodieBaseFile> baseFileIterator = limit(hoodieBaseFileIterator, maxSize);
        while (baseFileIterator.hasNext()) {
            HoodieBaseFile baseFile = baseFileIterator.next();
            splits.add(new HudiSplit(
                    baseFile.getPath(),
                    0L,
                    baseFile.getFileLen(),
                    baseFile.getFileSize(),
                    ImmutableList.of(),
                    tableHandle.getPredicate(),
                    getPartitionKeys()));
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

    private List<HivePartitionKey> getPartitionKeys()
    {
        List<HivePartitionKey> partitionKeys = new ArrayList<>();
        List<String> partitions = TimelineUtils.getPartitionsWritten(metaClient.getActiveTimeline());

        return partitionKeys;
    }
}
