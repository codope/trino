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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class HudiSplitSource
        implements ConnectorSplitSource
{
    private final HudiTableHandle tableHandle;

    public HudiSplitSource(HudiTableHandle tableHandle)
    {
        this.tableHandle = requireNonNull(tableHandle, "tableHandle is null");
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        List<ConnectorSplit> splits = new ArrayList<>();
        HudiSplit hudiSplit = new HudiSplit(
                "",
                0L,
                100L,
                100L,
                ImmutableList.of(),
                tableHandle.getPredicate(),
                getPartitionKeys(tableHandle));
        splits.add(hudiSplit);
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
        // TODO: finish based on file iterator
        return false;
    }

    private static List<HivePartitionKey> getPartitionKeys(HudiTableHandle hudiTableHandle)
    {
        List<HivePartitionKey> partitionKeys = new ArrayList<>();
        return partitionKeys;
    }
}
