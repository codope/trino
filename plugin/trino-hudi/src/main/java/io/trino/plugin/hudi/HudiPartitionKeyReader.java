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

import io.airlift.log.Logger;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class HudiPartitionKeyReader
        implements Runnable
{
    private static final Logger LOG = Logger.get(HudiPartitionKeyReader.class);
    private final Table table;
    private final HiveIdentity identity;
    private final HiveMetastore metastore;
    private final HoodieTableFileSystemView fileSystemView;
    private final ArrayDeque<List<String>> partitionNamesBuffer;
    private final Map<String, List<HivePartitionKey>> partitionToKeysMap;
    private final ArrayDeque<Pair<HoodieBaseFile, String>> hoodieFilesBuffer;

    public HudiPartitionKeyReader(
            Table table,
            HiveIdentity identity,
            HiveMetastore metastore,
            HoodieTableFileSystemView fileSystemView,
            ArrayDeque<List<String>> partitionNamesBuffer,
            Map<String, List<HivePartitionKey>> partitionToKeysMap,
            ArrayDeque<Pair<HoodieBaseFile, String>> hoodieFilesBuffer)
    {
        this.table = table;
        this.identity = identity;
        this.metastore = metastore;
        this.fileSystemView = fileSystemView;
        this.partitionNamesBuffer = partitionNamesBuffer;
        this.partitionToKeysMap = partitionToKeysMap;
        this.hoodieFilesBuffer = hoodieFilesBuffer;
    }

    @Override
    public void run()
    {
        HoodieTimer timer = new HoodieTimer().startTimer();
        List<String> partitionColumnNames = table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
        while (!partitionNamesBuffer.isEmpty()) {
            List<String> partitionNames = null;
            synchronized (partitionNamesBuffer) {
                if (!partitionNamesBuffer.isEmpty()) {
                    partitionNames = partitionNamesBuffer.pollFirst();
                }
            }

            if (partitionNames != null) {
                // Fetch partition keys
                Pair<String, List<HivePartitionKey>> partitionPair =
                        HudiUtil.getPartitionPathToKey(
                                identity, metastore, table, table.getStorage().getLocation(), partitionColumnNames, partitionNames);
                synchronized (partitionToKeysMap) {
                    partitionToKeysMap.put(partitionPair.getKey(), partitionPair.getValue());
                }
                LOG.warn(String.format("Add partition keys for %s: %s", partitionPair.getKey(), partitionPair.getValue()));
                List<Pair<HoodieBaseFile, String>> baseFiles = fileSystemView.getLatestBaseFiles(partitionPair.getKey())
                        .map(e -> new ImmutablePair<>(e, partitionPair.getKey())).collect(Collectors.toList());
                synchronized (hoodieFilesBuffer) {
                    hoodieFilesBuffer.addAll(baseFiles);
                }
                LOG.warn(String.format("Add %d base files for %s", baseFiles.size(), partitionPair.getKey()));
            }
        }
        LOG.warn(String.format("HudiPartitionKeyReader finishes in %d ms", timer.endTimer()));
    }
}
