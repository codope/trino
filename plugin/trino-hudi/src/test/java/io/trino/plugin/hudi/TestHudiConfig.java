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
import static org.apache.hudi.common.model.HoodieFileFormat.ORC;
import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;

public class TestHudiConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HudiConfig.class)
                .setFileFormat(PARQUET)
                .setMetadataEnabled(false)
                .setMaxSplitSize(DataSize.ofBytes(128 * 1024 * 1024))
                .setSkipMetaStoreForPartition(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hudi.file-format", "ORC")
                .put("hudi.max-split-size", "256MB")
                .put("hudi.metadata-enabled", "true")
                .put("hudi.skip-metastore-for-partition", "false")
                .build();

        HudiConfig expected = new HudiConfig()
                .setFileFormat(ORC)
                .setMaxSplitSize(DataSize.of(256, DataSize.Unit.MEGABYTE))
                .setMetadataEnabled(true)
                .setSkipMetaStoreForPartition(false);

        assertFullMapping(properties, expected);
    }
}
