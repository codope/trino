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

package io.trino.plugin.hudi.partition;

import io.airlift.log.Logger;
import io.trino.plugin.hudi.query.HudiFileListing;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HudiPartitionScanner
        implements Runnable
{
    private static final Logger LOG = Logger.get(HudiPartitionScanner.class);
    private final HudiFileListing hudiFileListing;
    private final ArrayDeque<HudiPartitionInfo> partitionQueue;
    private final Map<String, HudiPartitionInfo> partitionInfoMap;
    private final ArrayDeque<Pair<FileStatus, String>> hoodieFileStatusQueue;

    public HudiPartitionScanner(
            HudiFileListing hudiFileListing,
            ArrayDeque<HudiPartitionInfo> partitionQueue,
            Map<String, HudiPartitionInfo> partitionInfoMap,
            ArrayDeque<Pair<FileStatus, String>> hoodieFileStatusQueue)
    {
        this.hudiFileListing = hudiFileListing;
        this.partitionQueue = partitionQueue;
        this.partitionInfoMap = partitionInfoMap;
        this.hoodieFileStatusQueue = hoodieFileStatusQueue;
    }

    @Override
    public void run()
    {
        HoodieTimer timer = new HoodieTimer().startTimer();

        while (!partitionQueue.isEmpty()) {
            HudiPartitionInfo partitionInfo = null;
            synchronized (partitionQueue) {
                if (!partitionQueue.isEmpty()) {
                    partitionInfo = partitionQueue.pollFirst();
                }
            }

            if (partitionInfo != null) {
                // Load Hive partition keys
                partitionInfo.getHivePartitionKeys();
                synchronized (partitionInfoMap) {
                    partitionInfoMap.put(partitionInfo.getRelativePartitionPath(), partitionInfo);
                }
                final HudiPartitionInfo finalHudiPartitionInfo = partitionInfo;
                List<Pair<FileStatus, String>> fileStatusList = hudiFileListing.listStatus(partitionInfo).stream()
                        .map(fileStatus -> new ImmutablePair<>(
                                fileStatus, finalHudiPartitionInfo.getRelativePartitionPath()))
                        .collect(Collectors.toList());
                synchronized (hoodieFileStatusQueue) {
                    hoodieFileStatusQueue.addAll(fileStatusList);
                }
                LOG.debug(String.format("Add %d base files for %s",
                        fileStatusList.size(), partitionInfo.getRelativePartitionPath()));
            }
        }
        LOG.debug(String.format("HudiPartitionScanner %s finishes in %d ms", this, timer.endTimer()));
    }
}
