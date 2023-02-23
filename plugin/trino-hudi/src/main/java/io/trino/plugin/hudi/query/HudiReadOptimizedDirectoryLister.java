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
package io.trino.plugin.hudi.query;

import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.partition.HiveHudiPartitionInfo;
import io.trino.plugin.hudi.partition.HudiPartitionInfo;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hudi.HudiUtil.getFileStatus;

public class HudiReadOptimizedDirectoryLister
        implements HudiDirectoryLister
{
    private final HoodieTableFileSystemView fileSystemView;
    private final List<Column> partitionColumns;
    private final List<HudiPartitionInfo> allPartitionInfoList;

    public HudiReadOptimizedDirectoryLister(
            HoodieMetadataConfig metadataConfig,
            HoodieEngineContext engineContext,
            HudiTableHandle tableHandle,
            HoodieTableMetaClient metaClient,
            HiveMetastore hiveMetastore,
            Table hiveTable,
            List<HiveColumnHandle> partitionColumnHandles,
            List<String> hivePartitionNames)
    {
        this.fileSystemView = FileSystemViewManager.createInMemoryFileSystemView(engineContext, metaClient, metadataConfig);
        this.partitionColumns = hiveTable.getPartitionColumns();
        this.allPartitionInfoList = hivePartitionNames.stream()
                .map(hivePartitionName -> new HiveHudiPartitionInfo(
                        hivePartitionName,
                        partitionColumns,
                        partitionColumnHandles,
                        tableHandle.getPartitionPredicates(),
                        hiveTable,
                        hiveMetastore))
                .collect(Collectors.toList());
    }

    @Override
    public List<FileStatus> listStatus(HudiPartitionInfo partitionInfo, String commitTime)
    {
        return fileSystemView.getLatestFileSlicesBeforeOrOn(
                        partitionInfo.getRelativePartitionPath(),
                        commitTime,
                        false)
                .map(FileSlice::getBaseFile)
                .filter(Option::isPresent)
                .map(baseFile -> getFileStatus(baseFile.get()))
                .collect(toImmutableList());
    }

    @Override
    public List<FileSlice> listFileSlice(HudiPartitionInfo partitionInfo, String commitTime)
    {
        return fileSystemView.getLatestFileSlicesBeforeOrOn(
                        partitionInfo.getRelativePartitionPath(),
                        commitTime,
                        false)
                .collect(toImmutableList());
    }

    @Override
    public Optional<HudiPartitionInfo> getPartitionInfo(String partition)
    {
        return allPartitionInfoList.stream()
                .filter(partitionInfo -> partition.equals(partitionInfo.getHivePartitionName()))
                .findFirst();
    }

    @Override
    public void close()
    {
        if (fileSystemView != null && !fileSystemView.isClosed()) {
            fileSystemView.close();
        }
    }
}
