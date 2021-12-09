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

import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartition;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static io.trino.plugin.hive.util.HiveUtil.hiveColumnHandles;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_UNKNOWN_TABLE_TYPE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static org.apache.hudi.common.fs.FSUtils.getFs;
import static org.apache.hudi.common.table.HoodieTableMetaClient.METAFOLDER_NAME;
import static org.apache.hudi.exception.TableNotFoundException.checkTableValidity;

public class HudiMetadata
        implements ConnectorMetadata
{
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;

    public HudiMetadata(HiveMetastore metastore, HdfsEnvironment hdfsEnvironment, TypeManager typeManager)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return metastore.getAllDatabases();
    }

    @Override
    public HudiTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        //HudiTableHandle handle = HudiTableHandle.from(tableName);
        Table table = metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));
        if (!isHudiTable(session, table)) {
            throw new TrinoException(HUDI_UNKNOWN_TABLE_TYPE, format("Not a Hudi table: %s", tableName));
        }
        return new HudiTableHandle(
                tableName.getSchemaName(),
                tableName.getTableName(),
                HoodieTableType.COPY_ON_WRITE,
                TupleDomain.all(),
                Optional.empty(),
                getTableMetaClient(session, table));
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SchemaTableName tableName = ((HudiTableHandle) tableHandle).getSchemaTableName();
        Table table = metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));
        return hiveColumnHandles(table, typeManager, getTimestampPrecision(session)).stream()
                .collect(toImmutableMap(HiveColumnHandle::getName, identity()));
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle table)
    {
        return ((HudiTableHandle) table).getPartitions()
                .map(partitions -> new HudiInputInfo(
                        partitions.stream()
                                .map(HivePartition::getPartitionId)
                                .collect(toImmutableList()),
                        false));
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    public HiveMetastore getMetastore()
    {
        return metastore;
    }

    public HoodieTableMetaClient getTableMetaClient(ConnectorSession session, SchemaTableName tableName)
    {
        Table table = metastore.getTable(new HiveIdentity(session), tableName.getSchemaName(), tableName.getTableName())
                .orElseThrow(() -> new TableNotFoundException(tableName));
        String basePath = table.getStorage().getLocation();
        Configuration conf = hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session), new Path(basePath));
        return HoodieTableMetaClient.builder().setConf(conf).setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build();
    }

    public HoodieTableMetaClient getTableMetaClient(ConnectorSession session, Table table)
    {
        String basePath = table.getStorage().getLocation();
        Configuration conf = hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session), new Path(basePath));
        return HoodieTableMetaClient.builder().setConf(conf).setBasePath(basePath).setLoadActiveTimelineOnLoad(true).build();
    }

    public void rollback()
    {
        // TODO: cleanup open transaction
    }

    private boolean isHudiTable(ConnectorSession session, Table table)
    {
        String basePath = table.getStorage().getLocation();
        Configuration conf = hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session), new Path(basePath));
        try {
            checkTableValidity(getFs(basePath, conf), new Path(basePath), new Path(basePath, METAFOLDER_NAME));
        }
        catch (Exception e) {
            return false;
        }
        return true;
    }
}
