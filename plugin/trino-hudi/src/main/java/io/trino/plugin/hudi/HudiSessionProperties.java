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
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;

import javax.inject.Inject;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.enumProperty;

public class HudiSessionProperties
        implements SessionPropertiesProvider
{
    private static final String FILE_FORMAT = "file_format";
    private static final String METADATA_ENABLED = "metadata_enabled";

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
                        false));
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
}
