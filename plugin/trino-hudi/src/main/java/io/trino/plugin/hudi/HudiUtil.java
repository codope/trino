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

import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;

import java.util.Optional;
import java.util.Properties;

import static io.trino.plugin.hive.metastore.MetastoreUtil.getHiveSchema;

public class HudiUtil
{
    private HudiUtil() {}

    public static HoodieTableMetaClient getMetaClient(Configuration conf, String basePath)
    {
        return HoodieTableMetaClient.builder().setConf(conf).setBasePath(basePath).build();
    }

    public static boolean isHudiParquetInputFormat(InputFormat<?, ?> inputFormat)
    {
        return inputFormat instanceof HoodieParquetInputFormat;
    }

    public static Properties getPartitionSchema(Table table, Optional<Partition> partition)
    {
        if (partition.isEmpty()) {
            return getHiveSchema(table);
        }
        return getHiveSchema(partition.get(), table);
    }
}
