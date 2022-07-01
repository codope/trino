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
package io.trino.plugin.hudi.page;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.type.Type;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.exception.HoodieException;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class HudiLogConnectorPageSource implements ConnectorPageSource {
    private static final int ROWS_PER_REQUEST = 4096;
    private boolean closed;
    Schema schema;
    private final List<Type> types;
    private final PageBuilder pageBuilder;
    HoodieMergedLogRecordScanner hoodieMergedLogRecordScanner;
    final Iterator<String> logRecordsKeyIterator;

    public HudiLogConnectorPageSource(Schema schema, List<Type> types, HoodieMergedLogRecordScanner hoodieMergedLogRecordScanner) throws IOException {
        this.schema = schema;
        this.types = types;
        this.pageBuilder = new PageBuilder(this.types);
        this.hoodieMergedLogRecordScanner = hoodieMergedLogRecordScanner;
        logRecordsKeyIterator = hoodieMergedLogRecordScanner.getRecords().keySet().iterator();
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public boolean isFinished() {
        return closed && pageBuilder.isEmpty();
    }

    @Override
    public Page getNextPage() {
        if (!closed) {
            for (int i = 0; i < ROWS_PER_REQUEST && !pageBuilder.isFull(); i++) {
                if (!logRecordsKeyIterator.hasNext()) {
                    closed = true;
                    break;
                }
                String curAvroKey = logRecordsKeyIterator.next();
                Option<IndexedRecord> curAvroRecordOption = null;
                final HoodieAvroRecord<?> hoodieRecord = (HoodieAvroRecord) hoodieMergedLogRecordScanner.getRecords().get(curAvroKey);
                try {
                    curAvroRecordOption = hoodieRecord.getData().getInsertValue(schema);
                } catch (IOException e) {
                    throw new HoodieException("Get avro insert value error for key: " + curAvroKey, e);
                }
                if (curAvroRecordOption.isPresent()) {
                    final IndexedRecord curAvroRecord = curAvroRecordOption.get();
                    pageBuilder.declarePosition();
                    for (int column = 0; column < types.size(); column++) {
                        BlockBuilder output = pageBuilder.getBlockBuilder(column);
                        Type type = types.get(column);
                        Class<?> javaType = type.getJavaType();
                        if (javaType == boolean.class) {
                            type.writeBoolean(output, (Boolean) curAvroRecord.get(column));
                        } else if (javaType == long.class) {
                            type.writeLong(output, (Long) curAvroRecord.get(column));
                        } else if (javaType == double.class) {
                            type.writeDouble(output, (Double) curAvroRecord.get(column));
                        } else if (javaType == Slice.class) {
                            Utf8 utf8 = (Utf8) curAvroRecord.get(column);
                            Slice slice = Slices.wrappedBuffer(utf8.getBytes());
                            type.writeSlice(output, slice, 0, slice.length());
                        } else {
                            type.writeObject(output, curAvroRecord.get(column));
                        }
                    }
                }
            }
        }
        if ((closed && !pageBuilder.isEmpty()) || pageBuilder.isFull()) {
            Page page = pageBuilder.build();
            pageBuilder.reset();
            return page;
        }
        return null;
    }

    @Override
    public long getMemoryUsage() {
        ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records = (ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>>) hoodieMergedLogRecordScanner.getRecords();
        return records.getCurrentInMemoryMapSize() + records.getSizeOfFileOnDiskInBytes() + pageBuilder.getSizeInBytes();
    }

    @Override
    public void close() throws IOException {
        hoodieMergedLogRecordScanner.close();
    }
}
