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
package io.trino.plugin.hive.schema;

import org.apache.hudi.common.util.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class HoodieSchemaHistory
{
    Map<String, HoodieSchemaMetadata> fullSchemaHistory;
    Map<String, SchemaModification> addColumnHistory;
    Map<String, SchemaModification> dropColumnHistory;
    Map<String, SchemaModification> renameColumnHistory;

    public HoodieSchemaHistory()
    {
        this.fullSchemaHistory = new HashMap<>();
        this.addColumnHistory = new HashMap<>();
        this.dropColumnHistory = new HashMap<>();
        this.renameColumnHistory = new HashMap<>();
    }

    public void putSchemaHistory(HoodieSchemaMetadata schemaMetadata)
    {
        if (StringUtils.isNullOrEmpty(schemaMetadata.commitTime)) {
            return;
        }

        String commitTime = schemaMetadata.commitTime;
        SchemaModification schemaModification = schemaMetadata.schemaModification;
        fullSchemaHistory.put(commitTime, schemaMetadata);

        if (!Objects.isNull(schemaModification)) {
            switch (schemaModification.schemaModificationType) {
                case ADD_COLUMN:
                    addColumnHistory.put(commitTime, schemaModification);
                    break;
                case DROP_COLUMN:
                    dropColumnHistory.put(commitTime, schemaModification);
                    break;
                case ALTER_COLUMN_NAME:
                    renameColumnHistory.put(commitTime, schemaModification);
                    break;
                default:
                    throw new IllegalStateException("Unsupported schema evolution");
            }
        }
    }

    public Map<String, HoodieSchemaMetadata> getFullSchemaHistory()
    {
        String commitTime1 = "001";
        HoodieField aField = new HoodieField("uid", "BIGINT", "null");
        HoodieField bField = new HoodieField("old_name", "STRING", "null");
        HoodieField cField = new HoodieField("age", "INT", "null");
        List<HoodieField> fields = new ArrayList<>(Arrays.asList(aField, bField, cField));
        HoodieSchemaMetadata schema1 = new HoodieSchemaMetadata(commitTime1, fields, null);
        putSchemaHistory(schema1);

        // add a column
        String commitTime2 = "002";
        HoodieField dField = new HoodieField("state", "STRING", "null");
        fields.add(dField);
        SchemaModification schemaModification = new SchemaModification(SchemaModificationType.ADD_COLUMN, null, dField);
        HoodieSchemaMetadata schema2 = new HoodieSchemaMetadata(commitTime2, fields, schemaModification);
        putSchemaHistory(schema2);

        // drop a column
        String commitTime3 = "003";
        fields.remove(cField);
        schemaModification = new SchemaModification(SchemaModificationType.DROP_COLUMN, cField, null);
        HoodieSchemaMetadata schema3 = new HoodieSchemaMetadata(commitTime3, fields, schemaModification);
        putSchemaHistory(schema3);

        // rename a column
        String commitTime4 = "004";
        HoodieField aFieldRenamed = new HoodieField("new_name", "STRING", "null");
        fields.set(1, aFieldRenamed);
        schemaModification = new SchemaModification(SchemaModificationType.ALTER_COLUMN_NAME, aField, aFieldRenamed);
        HoodieSchemaMetadata schema4 = new HoodieSchemaMetadata(commitTime4, fields, schemaModification);
        putSchemaHistory(schema4);

        return fullSchemaHistory;
    }
}
