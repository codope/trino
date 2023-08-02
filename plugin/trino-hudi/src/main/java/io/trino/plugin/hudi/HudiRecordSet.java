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

import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class HudiRecordSet
        implements RecordSet
{
    private final List<HiveColumnHandle> columnHandles;

    public HudiRecordSet(HudiSplit split, List<HiveColumnHandle> columnHandles)
    {
        this.columnHandles = columnHandles;
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnHandles.stream().map(HiveColumnHandle::getType).collect(toImmutableList());
    }

    @Override
    public RecordCursor cursor()
    {
        return null;
    }
}
