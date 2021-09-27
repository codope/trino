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
import org.apache.hudi.common.model.HoodieFileFormat;

import javax.validation.constraints.NotNull;

import java.util.TimeZone;

import static io.trino.plugin.hudi.HudiFileFormat.PARQUET;

public class HudiConfig
{
    private HudiFileFormat fileFormat = PARQUET;
    private String parquetTimeZone = TimeZone.getDefault().getID();

    @NotNull
    public HoodieFileFormat getFileFormat()
    {
        return HoodieFileFormat.valueOf(fileFormat.name());
    }

    @Config("hudi.file-format")
    public HudiConfig setFileFormat(HudiFileFormat fileFormat)
    {
        this.fileFormat = fileFormat;
        return this;
    }

    @NotNull
    public String getParquetTimeZone()
    {
        return parquetTimeZone;
    }

    @Config("hudi.parquet.time-zone")
    @ConfigDescription("Time zone for Parquet read and write")
    public HudiConfig setParquetTimeZone(String parquetTimeZone)
    {
        this.parquetTimeZone = parquetTimeZone;
        return this;
    }
}
