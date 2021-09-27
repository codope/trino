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
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class HudiMetadata
        implements ConnectorMetadata
{
    private final HiveMetastore metastore;
    private final HdfsEnvironment hdfsEnvironment;
    private final HoodieTableMetaClient metaClient;

    public HudiMetadata(HiveMetastore metastore, HdfsEnvironment hdfsEnvironment)
    {
        this(metastore, hdfsEnvironment, getTableMetaClient());
    }

    private static HoodieTableMetaClient getTableMetaClient()
    {
        return null;
    }

    public HudiMetadata(HiveMetastore metastore, HdfsEnvironment hdfsEnvironment, HoodieTableMetaClient metaClient)
    {
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.metaClient = requireNonNull(metaClient, "metaClient is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return null;
    }

    public HiveMetastore getMetastore()
    {
        return metastore;
    }

    public void rollback()
    {
        // TODO: cleanup open transaction
    }
}
