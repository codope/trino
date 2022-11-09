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

import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.Optional;

import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.memoizeMetastore;
import static java.util.Objects.requireNonNull;

public class HudiMetadataFactory
{
    private final HiveMetastoreFactory metastoreFactory;
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeManager typeManager;
    private final HudiPartitionManager partitionManager;

    @Inject
    public HudiMetadataFactory(
            HiveMetastoreFactory metastoreFactory,
            HdfsEnvironment hdfsEnvironment,
            TypeManager typeManager,
            HudiPartitionManager partitionManager)
    {
        this.metastoreFactory = requireNonNull(metastoreFactory, "metastore is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.partitionManager = requireNonNull(partitionManager, "partitionManager is null");
    }

    public HudiMetadata create(ConnectorIdentity identity)
    {
        HiveMetastore metastore = metastoreFactory.createMetastore(Optional.of(identity));
        // create per-transaction cache over hive metastore interface
        CachingHiveMetastore cachingHiveMetastore = memoizeMetastore(
                metastoreFactory.createMetastore(Optional.of(identity)),
                2000);
        return new HudiMetadata(cachingHiveMetastore, hdfsEnvironment, typeManager, partitionManager, new HudiHiveStatisticsProvider(metastore));
    }
}
