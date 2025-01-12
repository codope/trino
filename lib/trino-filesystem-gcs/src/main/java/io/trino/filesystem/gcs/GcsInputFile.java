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
package io.trino.filesystem.gcs;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;

import java.io.IOException;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.filesystem.gcs.GcsUtils.getBlob;
import static io.trino.filesystem.gcs.GcsUtils.getBlobOrThrow;
import static io.trino.filesystem.gcs.GcsUtils.handleGcsException;
import static java.util.Objects.requireNonNull;

public class GcsInputFile
        implements TrinoInputFile
{
    private final GcsLocation location;
    private final Storage storage;
    private final int readBlockSize;
    private OptionalLong length;
    private OptionalLong predeclaredLength;
    private Optional<Instant> lastModified = Optional.empty();

    public GcsInputFile(GcsLocation location, Storage storage, int readBockSize, OptionalLong predeclaredLength)
    {
        this.location = requireNonNull(location, "location is null");
        this.storage = requireNonNull(storage, "storage is null");
        this.readBlockSize = readBockSize;
        this.predeclaredLength = requireNonNull(predeclaredLength, "length is null");
        this.length = OptionalLong.empty();
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        // Note: Only pass predeclared length, to keep the contract of TrinoFileSystem.newInputFile
        return new GcsInput(location, storage, readBlockSize, predeclaredLength);
    }

    @Override
    public TrinoInputStream newStream()
            throws IOException
    {
        Blob blob = getBlobOrThrow(storage, location);
        return new GcsInputStream(location, blob, readBlockSize, predeclaredLength);
    }

    @Override
    public long length()
            throws IOException
    {
        if (predeclaredLength.isPresent()) {
            return predeclaredLength.getAsLong();
        }
        if (length.isEmpty()) {
            loadProperties();
        }
        return length.orElseThrow();
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        if (lastModified.isEmpty()) {
            loadProperties();
        }
        return lastModified.orElseThrow();
    }

    @Override
    public boolean exists()
            throws IOException
    {
        Optional<Blob> blob = getBlob(storage, location);
        return blob.isPresent() && blob.get().exists();
    }

    @Override
    public Location location()
    {
        return location.location();
    }

    private void loadProperties()
            throws IOException
    {
        Blob blob = getBlobOrThrow(storage, location);
        try {
            length = OptionalLong.of(blob.getSize());
            lastModified = Optional.of(Instant.from(blob.getUpdateTimeOffsetDateTime()));
        }
        catch (RuntimeException e) {
            throw handleGcsException(e, "fetching properties for file", location);
        }
    }
}
