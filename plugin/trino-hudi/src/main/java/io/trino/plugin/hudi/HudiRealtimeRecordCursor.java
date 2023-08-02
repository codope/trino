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

import io.airlift.slice.Slice;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.Type;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.hadoop.realtime.HoodieMergeOnReadSnapshotReader;
import org.apache.hudi.io.storage.HoodieFileReader;

public class HudiRealtimeRecordCursor
        implements RecordCursor
{
    private final HoodieMergedLogRecordScanner logRecordScanner;
    private final HoodieFileReader baseFileReader;
    private final HoodieMergeOnReadSnapshotReader snapshotReader;

    public HudiRealtimeRecordCursor(
            HoodieMergedLogRecordScanner logRecordScanner,
            HoodieFileReader baseFileReader,
            HoodieMergeOnReadSnapshotReader snapshotReader)
    {
        this.logRecordScanner = logRecordScanner;
        this.baseFileReader = baseFileReader;
        this.snapshotReader = snapshotReader;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        return null;
    }

    @Override
    public boolean advanceNextPosition()
    {
        return false;
    }

    @Override
    public boolean getBoolean(int field)
    {
        return false;
    }

    @Override
    public long getLong(int field)
    {
        return 0;
    }

    @Override
    public double getDouble(int field)
    {
        return 0;
    }

    @Override
    public Slice getSlice(int field)
    {
        return null;
    }

    @Override
    public Object getObject(int field)
    {
        return null;
    }

    @Override
    public boolean isNull(int field)
    {
        return false;
    }

    @Override
    public void close()
    {
        if (logRecordScanner != null) {
            logRecordScanner.close();
        }
    }
}
