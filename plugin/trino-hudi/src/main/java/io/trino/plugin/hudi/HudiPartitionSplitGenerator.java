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
import io.trino.spi.connector.ConnectorSplit;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HudiPartitionSplitGenerator
        implements Runnable
{
    private static final Logger LOG = Logger.get(HudiPartitionSplitGenerator.class);
    private final FileSystem fileSystem;
    private final HoodieTableMetaClient metaClient;
    private final HudiTableHandle tableHandle;
    private final Map<String, List<HivePartitionKey>> partitionToKeysMap;
    private final ArrayDeque<Pair<HoodieBaseFile, String>> hoodieFilesBuffer;
    private final ArrayDeque<ConnectorSplit> connectorSplitBuffer;
    private boolean isRunning;

    public HudiPartitionSplitGenerator(
            FileSystem fileSystem,
            HoodieTableMetaClient metaClient,
            HudiTableHandle tableHandle,
            Map<String, List<HivePartitionKey>> partitionToKeysMap,
            ArrayDeque<Pair<HoodieBaseFile, String>> hoodieFilesBuffer,
            ArrayDeque<ConnectorSplit> connectorSplitBuffer)
    {
        this.fileSystem = fileSystem;
        this.metaClient = metaClient;
        this.tableHandle = tableHandle;
        this.partitionToKeysMap = partitionToKeysMap;
        this.hoodieFilesBuffer = hoodieFilesBuffer;
        this.connectorSplitBuffer = connectorSplitBuffer;
        this.isRunning = true;
    }

    @Override
    public void run()
    {
        HoodieTimer timer = new HoodieTimer().startTimer();
        while (isRunning || !hoodieFilesBuffer.isEmpty()) {
            Pair<HoodieBaseFile, String> baseFilePair = null;
            synchronized (hoodieFilesBuffer) {
                if (!hoodieFilesBuffer.isEmpty()) {
                    baseFilePair = hoodieFilesBuffer.pollFirst();
                }
            }
            if (baseFilePair != null) {
                try {
                    String relativePartitionPath = baseFilePair.getValue();
                    List<HudiSplit> hudiSplits = HudiUtil.getSplits(
                                    fileSystem, HoodieInputFormatUtils.getFileStatus(baseFilePair.getKey()))
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
                                            partitionToKeysMap.get(relativePartitionPath)));
                                }
                                catch (IOException e) {
                                    throw new HoodieIOException(String.format(
                                            "Unable to get Hudi split for %s, start=%d len=%d",
                                            fileSplit.getPath(), fileSplit.getStart(), fileSplit.getLength()), e);
                                }
                                return result.stream();
                            })
                            .collect(Collectors.toList());
                    synchronized (connectorSplitBuffer) {
                        connectorSplitBuffer.addAll(hudiSplits);
                    }
                    //LOG.warn(String.format("Add %d splits for %s", hudiSplits.size(), baseFilePair.getKey().getPath()));
                }
                catch (IOException e) {
                    throw new HoodieIOException("Unable to get splits for " + baseFilePair.getKey().getPath(), e);
                }
            }
        }
        LOG.warn(String.format("HudiPartitionSplitGenerator finishes in %d ms", timer.endTimer()));
    }

    public void stopRunning()
    {
        this.isRunning = false;
    }
}
