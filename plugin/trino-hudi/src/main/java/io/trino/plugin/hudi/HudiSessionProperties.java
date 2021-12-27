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

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;

import javax.inject.Inject;

import java.util.List;

import static io.trino.plugin.base.session.PropertyMetadataUtil.dataSizeProperty;
import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;

public class HudiSessionProperties
        implements SessionPropertiesProvider
{
    private static final String FILE_FORMAT = "file_format";
    private static final String METADATA_ENABLED = "metadata_enabled";
    private static final String MAX_SPLIT_SIZE = "max_split_size";
    private static final String SKIP_METASTORE_FOR_PARTITION = "skip_metastore_for_partition";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public HudiSessionProperties(HudiConfig hudiConfig)
    {
        sessionProperties = ImmutableList.of(
                enumProperty(
                        FILE_FORMAT,
                        "Currently, only Parquet is supported",
                        HoodieFileFormat.class,
                        hudiConfig.getFileFormat(),
                        false),
                booleanProperty(
                        METADATA_ENABLED,
                        "For Hudi tables prefer to fetch the list of files from its metadata",
                        hudiConfig.isMetadataEnabled(),
                        false),
                booleanProperty(
                        SKIP_METASTORE_FOR_PARTITION,
                        "Whether to skip metastore for partition.",
                        hudiConfig.getSkipMetaStoreForPartition(),
                        false),
                dataSizeProperty(
                        MAX_SPLIT_SIZE,
                        "Max split size",
                        hudiConfig.getMaxSplitSize(),
                        true));
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static HoodieFileFormat getFileFormat(ConnectorSession session)
    {
        return session.getProperty(FILE_FORMAT, HoodieFileFormat.class);
    }

    public static boolean isHudiMetadataEnabled(ConnectorSession session)
    {
        return session.getProperty(METADATA_ENABLED, Boolean.class);
    }

    public static DataSize getMaxSplitSize(ConnectorSession session)
    {
        return session.getProperty(MAX_SPLIT_SIZE, DataSize.class);
    }

    public static boolean shouldSkipMetaStoreForPartition(ConnectorSession session)
    {
        return session.getProperty(SKIP_METASTORE_FOR_PARTITION, Boolean.class);
    }
}
