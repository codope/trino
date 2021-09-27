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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.HoodieTableType;

import java.util.Locale;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HudiTableHandle
        implements ConnectorTableHandle
{
    private static final Pattern TABLE_PATTERN = Pattern.compile("" +
            "(?<table>[^$@]+)" +
            "(?:@(?<ver1>[0-9]+))?" +
            "(?:\\$(?<type>[^@]+)(?:@(?<ver2>[0-9]+))?)?");

    private final String schemaName;
    private final String tableName;
    private final HoodieTableType tableType;
    private final Optional<Long> snapshotId;
    private final TupleDomain<HiveColumnHandle> predicate;

    @JsonCreator
    public HudiTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("tableType") HoodieTableType tableType,
            @JsonProperty("snapshotId") Optional<Long> snapshotId,
            @JsonProperty("predicate") TupleDomain<HiveColumnHandle> predicate)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.snapshotId = requireNonNull(snapshotId, "snapshotId is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public HoodieTableType getTableType()
    {
        return tableType;
    }

    @JsonProperty
    public Optional<Long> getSnapshotId()
    {
        return snapshotId;
    }

    @JsonProperty
    public TupleDomain<HiveColumnHandle> getPredicate()
    {
        return predicate;
    }

    public SchemaTableName getSchemaTableName()
    {
        return new SchemaTableName(schemaName, tableName);
    }

    @Override
    public String toString()
    {
        return getSchemaTableName().toString();
    }

    public static HudiTableHandle from(SchemaTableName name)
    {
        Matcher match = TABLE_PATTERN.matcher(name.getTableName());
        if (!match.matches()) {
            throw new TrinoException(NOT_SUPPORTED, "Invalid Hudi table name: " + name);
        }

        String table = match.group("table");
        String typeString = match.group("type");
        String ver1 = match.group("ver1");
        String ver2 = match.group("ver2");

        HoodieTableType type = HoodieTableType.COPY_ON_WRITE;
        if (typeString != null) {
            try {
                type = HoodieTableType.valueOf(typeString.toUpperCase(Locale.ROOT));
            }
            catch (IllegalArgumentException e) {
                throw new TrinoException(NOT_SUPPORTED, format("Invalid Hudi table name (unknown type '%s'): %s", typeString, name));
            }
        }

        Optional<Long> version = Optional.empty();
        if (type == HoodieTableType.COPY_ON_WRITE || type == HoodieTableType.MERGE_ON_READ) {
            if (ver1 != null && ver2 != null) {
                throw new TrinoException(NOT_SUPPORTED, "Invalid Hudi table name (cannot specify two @ versions): " + name);
            }
            if (ver1 != null) {
                version = Optional.of(parseLong(ver1));
            }
            else if (ver2 != null) {
                version = Optional.of(parseLong(ver2));
            }
        }
        else if (ver1 != null || ver2 != null) {
            throw new TrinoException(NOT_SUPPORTED, format("Invalid Hudi table name (cannot use @ version with table type '%s'): %s", type, name));
        }

        return new HudiTableHandle(name.getSchemaName(), table, type, version, TupleDomain.all());
    }
}
