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

import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.apache.hudi.common.model.HoodieFileFormat.ORC;
import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;

public class TestHudiConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HudiConfig.class)
                .setBaseFileFormat(PARQUET)
                .setMetadataEnabled(false)
                .setSkipMetaStoreForPartition(false)
                .setUseParquetColumnNames(true)
                .setPartitionScannerParallelism(16)
                .setSplitGeneratorParallelism(16)
                .setMinPartitionBatchSize(10)
                .setMaxPartitionBatchSize(100)
                .setSizeBasedSplitWeightsEnabled(true)
                .setStandardSplitWeightSize(DataSize.of(128, MEGABYTE))
                .setMinimumAssignedSplitWeight(0.05));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hudi.base-file-format", "ORC")
                .put("hudi.metadata-enabled", "true")
                .put("hudi.skip-metastore-for-partition", "true")
                .put("hudi.use-parquet-column-names", "false")
                .put("hudi.partition-scanner-parallelism", "8")
                .put("hudi.split-generator-parallelism", "8")
                .put("hudi.min-partition-batch-size", "5")
                .put("hudi.max-partition-batch-size", "50")
                .put("hudi.size-based-split-weights-enabled", "false")
                .put("hudi.standard-split-weight-size", "64MB")
                .put("hudi.minimum-assigned-split-weight", "0.1")
                .build();

        HudiConfig expected = new HudiConfig()
                .setBaseFileFormat(ORC)
                .setMetadataEnabled(true)
                .setSkipMetaStoreForPartition(true)
                .setUseParquetColumnNames(false)
                .setPartitionScannerParallelism(8)
                .setSplitGeneratorParallelism(8)
                .setMinPartitionBatchSize(5)
                .setMaxPartitionBatchSize(50)
                .setSizeBasedSplitWeightsEnabled(false)
                .setStandardSplitWeightSize(DataSize.of(64, MEGABYTE))
                .setMinimumAssignedSplitWeight(0.1);

        assertFullMapping(properties, expected);
    }
}
