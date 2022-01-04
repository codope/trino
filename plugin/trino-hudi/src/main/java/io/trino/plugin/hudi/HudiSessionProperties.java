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
import static io.trino.spi.session.PropertyMetadata.integerProperty;

public class HudiSessionProperties
        implements SessionPropertiesProvider
{
    private static final String FILE_FORMAT = "file_format";
    private static final String METADATA_ENABLED = "metadata_enabled";
    private static final String SKIP_METASTORE_FOR_PARTITION = "skip_metastore_for_partition";
    private static final String USE_PARQUET_COLUMN_NAMES = "use_parquet_column_names";
    private static final String PARTITION_SCANNER_PARALLELISM = "partition_scanner_parallelism";
    private static final String SPLIT_GENERATOR_PARALLELISM = "split_generator_parallelism";

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
                booleanProperty(
                        USE_PARQUET_COLUMN_NAMES,
                        "Whether to use column names from parquet files.",
                        hudiConfig.getUseParquetColumnNames(),
                        false),
                integerProperty(
                        PARTITION_SCANNER_PARALLELISM,
                        "Number of threads to use for partition scanners",
                        hudiConfig.getPartitionScannerParallelism(),
                        false),
                integerProperty(
                        SPLIT_GENERATOR_PARALLELISM,
                        "Number of threads to use for split generators",
                        hudiConfig.getSplitGeneratorParallelism(),
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

    public static boolean shouldSkipMetaStoreForPartition(ConnectorSession session)
    {
        return session.getProperty(SKIP_METASTORE_FOR_PARTITION, Boolean.class);
    }

    public static boolean shouldUseParquetColumnNames(ConnectorSession session)
    {
        return session.getProperty(USE_PARQUET_COLUMN_NAMES, Boolean.class);
    }

    public static int getPartitionScannerParallelism(ConnectorSession session)
    {
        return session.getProperty(PARTITION_SCANNER_PARALLELISM, Integer.class);
    }

    public static int getSplitGeneratorParallelism(ConnectorSession session)
    {
        return session.getProperty(SPLIT_GENERATOR_PARALLELISM, Integer.class);
    }
}
