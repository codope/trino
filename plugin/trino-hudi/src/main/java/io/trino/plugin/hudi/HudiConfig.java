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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.DataSize;
import org.apache.hudi.common.model.HoodieFileFormat;

import javax.validation.constraints.NotNull;

import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;

public class HudiConfig
{
    private HoodieFileFormat fileFormat = PARQUET;
    private boolean metadataEnabled;
    private boolean shouldSkipMetaStoreForPartition = true;
    private DataSize maxSplitSize = DataSize.ofBytes(128 * 1024 * 1024);

    @NotNull
    public HoodieFileFormat getFileFormat()
    {
        return HoodieFileFormat.valueOf(fileFormat.name());
    }

    @Config("hudi.file-format")
    public HudiConfig setFileFormat(HoodieFileFormat fileFormat)
    {
        this.fileFormat = fileFormat;
        return this;
    }

    @Config("hudi.metadata-enabled")
    @ConfigDescription("Fetch the list of file names and sizes from metadata rather than storage")
    public HudiConfig setMetadataEnabled(boolean metadataEnabled)
    {
        this.metadataEnabled = metadataEnabled;
        return this;
    }

    @NotNull
    public boolean isMetadataEnabled()
    {
        return this.metadataEnabled;
    }

    @Config("hudi.max-split-size")
    public HudiConfig setMaxSplitSize(DataSize size)
    {
        this.maxSplitSize = size;
        return this;
    }

    @NotNull
    public DataSize getMaxSplitSize()
    {
        return this.maxSplitSize;
    }

    @Config("hudi.skip-metastore-for-partition")
    @ConfigDescription("Whether to skip metastore for partition")
    public HudiConfig setSkipMetaStoreForPartition(boolean shouldSkipMetaStoreForPartition)
    {
        this.shouldSkipMetaStoreForPartition = shouldSkipMetaStoreForPartition;
        return this;
    }

    @NotNull
    public boolean getSkipMetaStoreForPartition()
    {
        return this.shouldSkipMetaStoreForPartition;
    }
}
