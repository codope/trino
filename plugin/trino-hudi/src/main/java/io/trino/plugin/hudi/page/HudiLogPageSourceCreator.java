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

package io.trino.plugin.hudi.page;

import io.trino.plugin.hive.FileFormatDataSourceStats;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.HudiConfig;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.HoodieWrapperFileSystem;
import org.apache.hudi.common.fs.NoOpConsistencyGuard;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class HudiLogPageSourceCreator
        extends HudiPageSourceCreator {

    public HudiLogPageSourceCreator(HudiConfig hudiConfig, HdfsEnvironment hdfsEnvironment, FileFormatDataSourceStats stats, DateTimeZone timeZone) {
        super(hudiConfig, hdfsEnvironment, stats, timeZone);
    }

    @Override
    public ConnectorPageSource createPageSource(Configuration configuration, ConnectorIdentity identity, List<HiveColumnHandle> regularColumns, HudiSplit hudiSplit) {
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(identity, new Path(hudiSplit.getPath()), configuration);
            List<Type> collect = regularColumns.stream().map(hd -> hd.getBaseType()).collect(Collectors.toList());
            AtomicReference<Schema> schema = new AtomicReference<>();
            HoodieMergedLogRecordScanner hoodieMergedLogRecordScanner = hdfsEnvironment.doAs(identity, () -> {
                HoodieWrapperFileSystem hoodieWrapperFileSystem = new HoodieWrapperFileSystem(fileSystem, new NoOpConsistencyGuard());
                HoodieTableMetaClient client = HoodieTableMetaClient.builder().setConf(fileSystem.getConf()).setBasePath(hudiSplit.getBasePath()).build();
                client.getTableConfig().setValue("hoodie.bootstrap.index.enable", "false");
                String latestInstantTime = client.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().getWriteTimeline().lastInstant().get().getTimestamp();
                TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(client);
                Schema tableAvroSchema = tableSchemaResolver.getTableAvroSchema(true);
                schema.set(tableAvroSchema);
                return HoodieMergedLogRecordScanner.newBuilder()
                        .withFileSystem(hoodieWrapperFileSystem)
                        .withBasePath(hudiSplit.getBasePath())
                        .withLogFilePaths(List.of(hudiSplit.getPath()))
                        .withReaderSchema(tableAvroSchema)
                        .withLatestInstantTime(latestInstantTime)
                        .withReadBlocksLazily(Boolean.parseBoolean(HoodieRealtimeConfig.DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED))
                        .withReverseReader(false)
                        .withBufferSize(HoodieRealtimeConfig.DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE)
                        .withMaxMemorySizeInBytes(10 * 1024 * 1024L)
                        .withSpillableMapBasePath(HoodieRealtimeConfig.DEFAULT_SPILLABLE_MAP_BASE_PATH)
                        .withInstantRange(Option.empty())
                        .withOperationField(true)
                        .build();
            });
            return new HudiLogConnectorPageSource(schema.get(), collect, hoodieMergedLogRecordScanner);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
