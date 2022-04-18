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

package io.trino.plugin.hudi.split;

import com.google.common.collect.ImmutableList;
import io.airlift.concurrent.MoreFutures;
import io.trino.plugin.hive.HiveErrorCode;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.HudiUtil;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import io.trino.plugin.hudi.partition.HudiPartitionInfoLoader;
import io.trino.plugin.hudi.query.HudiFileListing;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.runAsync;

public class HudiSplitBackgroundLoader
        implements Runnable
{
    private final ConnectorSession session;
    private final HudiTableHandle tableHandle;
    private final HoodieTableMetaClient metaClient;
    private final HudiFileListing hudiFileListing;
    private final AsyncQueue<ConnectorSplit> asyncQueue;
    private final ExecutorService executor;
    private final HudiSplitWeightProvider hudiSplitWeightProvider;

    public HudiSplitBackgroundLoader(
            ConnectorSession session,
            HudiTableHandle tableHandle,
            HoodieTableMetaClient metaClient,
            HudiFileListing hudiFileListing,
            AsyncQueue<ConnectorSplit> asyncQueue,
            ExecutorService executor,
            HudiSplitWeightProvider hudiSplitWeightProvider)
    {
        this.session = session;
        this.tableHandle = tableHandle;
        this.metaClient = metaClient;
        this.hudiFileListing = hudiFileListing;
        this.asyncQueue = requireNonNull(asyncQueue, "asyncQueue is null");
        this.executor = requireNonNull(executor, "executor is null");
        this.hudiSplitWeightProvider = requireNonNull(hudiSplitWeightProvider, "hudiSplitWeightProvider is null");
    }

    @Override
    public void run()
    {
        ArrayDeque<HudiPartitionInfo> partitionQueue = new ArrayDeque<>();
        HudiPartitionInfoLoader partitionInfoLoader =
                new HudiPartitionInfoLoader(session, hudiFileListing, partitionQueue);
        partitionInfoLoader.run();

        CompletableFuture<?>[] futures = partitionQueue.stream()
                .map(this::loadPartitionSplitsAsync)
                .collect(Collectors.toList())
                .toArray(CompletableFuture<?>[]::new);
        CompletableFuture.allOf(futures).join();
        asyncQueue.finish();
    }

    private CompletableFuture<Void> loadPartitionSplitsAsync(HudiPartitionInfo partition)
    {
        return runAsync(() -> {
            List<HivePartitionKey> partitionKeys = partition.getHivePartitionKeys();
            List<FileStatus> partitionFiles = hudiFileListing.listStatus(partition);
            partitionFiles.stream()
                    .flatMap(fileStatus -> loadHudiSplits(fileStatus, partitionKeys))
                    .map(asyncQueue::offer)
                    .forEachOrdered(MoreFutures::getFutureValue);
        }, executor);
    }

    private Stream<HudiSplit> loadHudiSplits(FileStatus fileStatus, List<HivePartitionKey> hivePartitionKeys)
    {
        FileSystem fileSystem = metaClient.getFs();
        final List<FileSplit> splits;
        try {
            splits = HudiUtil.getSplits(fileSystem, fileStatus);
        }
        catch (IOException e) {
            throw new TrinoException(HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT, e);
        }

        return splits.stream()
                .map(fileSplit -> new HudiSplit(
                        fileSplit.getPath().toString(),
                        fileSplit.getStart(),
                        fileSplit.getLength(),
                        fileStatus.getLen(),
                        ImmutableList.of(),
                        tableHandle.getRegularPredicates(),
                        hivePartitionKeys,
                        hudiSplitWeightProvider.weightForSplitSizeInBytes(fileSplit.getLength())));
    }
}
