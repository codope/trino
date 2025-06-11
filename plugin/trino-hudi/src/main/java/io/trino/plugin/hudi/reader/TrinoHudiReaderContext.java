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
package io.trino.plugin.hudi.reader;

import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.util.HudiAvroSerializer;
import io.trino.plugin.hudi.util.SynthesizedColumnHandler;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieAvroIndexedRecord;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieEmptyRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.read.BufferedRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

public class TrinoHudiReaderContext
        extends HoodieReaderContext<IndexedRecord>
{
    private final ConnectorPageSource pageSource;
    private final HudiAvroSerializer avroSerializer;
    private final Map<String, Integer> columnToPosition;

    public TrinoHudiReaderContext(
            ConnectorPageSource pageSource,
            List<HiveColumnHandle> columnHandles,
            SynthesizedColumnHandler synthesizedColumnHandler)
    {
        this.pageSource = pageSource;
        this.avroSerializer = new HudiAvroSerializer(columnHandles, synthesizedColumnHandler);
        this.columnToPosition = new HashMap<>();
        for (int i = 0; i < columnHandles.size(); i++) {
            HiveColumnHandle handle = columnHandles.get(i);
            columnToPosition.put(handle.getBaseColumnName(), i);
        }
    }

    @Override
    public ClosableIterator<IndexedRecord> getFileRecordIterator(
            StoragePath storagePath,
            long start,
            long length,
            Schema dataSchema,
            Schema requiredSchema,
            HoodieStorage storage)
    {
        return new ClosableIterator<>()
        {
            private SourcePage currentPage;
            private int currentPosition;

            @Override
            public void close()
            {
                try {
                    pageSource.close();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean hasNext()
            {
                // If all records in the current page are consume, try to get next page
                if (currentPage == null || currentPosition >= currentPage.getPositionCount()) {
                    if (pageSource.isFinished()) {
                        return false;
                    }

                    // Get next page and reset currentPosition
                    currentPage = pageSource.getNextSourcePage();
                    currentPosition = 0;

                    // If no more pages are available
                    return currentPage != null;
                }

                return true;
            }

            @Override
            public IndexedRecord next()
            {
                if (!hasNext()) {
                    throw new RuntimeException("No more records in the iterator");
                }

                IndexedRecord record = avroSerializer.serialize(currentPage, currentPosition);
                currentPosition++;
                return record;
            }
        };
    }

    @Override
    public IndexedRecord convertAvroRecord(IndexedRecord record)
    {
        return record;
    }

    @Override
    public GenericRecord convertToAvroRecord(IndexedRecord record, Schema schema)
    {
        GenericRecord ret = new GenericData.Record(schema);
        for (Schema.Field field : schema.getFields()) {
            ret.put(field.name(), record.get(field.pos()));
        }
        return ret;
    }

    @Override
    public Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses)
    {
        return Option.of(HoodieAvroRecordMerger.INSTANCE);
    }

    @Override
    public Object getValue(IndexedRecord record, Schema schema, String fieldName)
    {
        if (columnToPosition.containsKey(fieldName)) {
            return record.get(columnToPosition.get(fieldName));
        }
        // record doesn't have the queried field, return null
        return null;
    }

    @Override
    public IndexedRecord seal(IndexedRecord record)
    {
        Schema schema = record.getSchema();
        IndexedRecord newRecord = new Record(schema);
        List<Schema.Field> fields = schema.getFields();
        for (Schema.Field field : fields) {
            int pos = schema.getField(field.name()).pos();
            newRecord.put(pos, record.get(pos));
        }
        return newRecord;
    }

    @Override
    public ClosableIterator<IndexedRecord> mergeBootstrapReaders(
            ClosableIterator closableIterator, Schema schema,
            ClosableIterator closableIterator1, Schema schema1)
    {
        return null;
    }

    @Override
    public UnaryOperator<IndexedRecord> projectRecord(
            Schema from,
            Schema to,
            Map<String, String> renamedColumns)
    {
        List<Schema.Field> toFields = to.getFields();
        int[] projection = new int[toFields.size()];
        for (int i = 0; i < projection.length; i++) {
            projection[i] = from.getField(toFields.get(i).name()).pos();
        }

        return fromRecord -> {
            IndexedRecord toRecord = new Record(to);
            for (int i = 0; i < projection.length; i++) {
                toRecord.put(i, fromRecord.get(projection[i]));
            }
            return toRecord;
        };
    }

    @Override
    public HoodieRecord<IndexedRecord> constructHoodieRecord(BufferedRecord<IndexedRecord> bufferedRecord)
    {
        if (bufferedRecord.isDelete()) {
            return new HoodieEmptyRecord<>(
                    new HoodieKey(bufferedRecord.getRecordKey(), null),
                    HoodieRecord.HoodieRecordType.AVRO);
        }
        return new HoodieAvroIndexedRecord(bufferedRecord.getRecord());
    }

    @Override
    public IndexedRecord toBinaryRow(Schema schema, IndexedRecord indexedRecord)
    {
        return indexedRecord;
    }
}
