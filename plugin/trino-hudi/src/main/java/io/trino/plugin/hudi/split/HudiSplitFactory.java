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

package io.trino.plugin.hudi.split;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.spi.TrinoException;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hudi.hadoop.PathWithBootstrapFileStatus;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HudiSplitFactory
{
    private static final double SPLIT_SLOP = 1.1;   // 10% slop/overflow allowed in bytes per split while generating splits

    private final HudiTableHandle hudiTableHandle;
    private final HudiSplitWeightProvider hudiSplitWeightProvider;
    private final FileSystem fileSystem;

    public HudiSplitFactory(
            HudiTableHandle hudiTableHandle,
            HudiSplitWeightProvider hudiSplitWeightProvider,
            FileSystem fileSystem)
    {
        this.hudiTableHandle = requireNonNull(hudiTableHandle, "hudiTableHandle is null");
        this.hudiSplitWeightProvider = requireNonNull(hudiSplitWeightProvider, "hudiSplitWeightProvider is null");
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
    }

    public Stream<HudiSplit> createSplits(List<HivePartitionKey> partitionKeys, FileStatus fileStatus)
    {
        final List<FileSplit> splits;
        try {
            splits = createSplits(fileStatus);
        }
        catch (IOException e) {
            throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, e);
        }

        return splits.stream()
                .map(fileSplit -> new HudiSplit(
                        fileSplit.getPath().toString(),
                        fileSplit.getStart(),
                        fileSplit.getLength(),
                        fileStatus.getLen(),
                        ImmutableList.of(),
                        hudiTableHandle.getRegularPredicates(),
                        partitionKeys,
                        hudiSplitWeightProvider.calculateSplitWeight(fileSplit.getLength()),
                        hudiTableHandle.getBasePath()));
    }

    private List<FileSplit> createSplits(FileStatus fileStatus)
            throws IOException
    {
        if (fileStatus.isDirectory()) {
            throw new IOException("Not a file: " + fileStatus.getPath());
        }

        Path path = fileStatus.getPath();
        long length = fileStatus.getLen();

        if (length == 0) {
            return ImmutableList.of(new FileSplit(path, 0, length, new String[0]));
        }

        BlockLocation[] blkLocations;
        if (fileStatus instanceof LocatedFileStatus) {
            blkLocations = ((LocatedFileStatus) fileStatus).getBlockLocations();
        }
        else {
            blkLocations = fileSystem.getFileBlockLocations(fileStatus, 0, length);
        }

        if (!isSplitable(path)) {
            SplitHosts splitHosts = SplitHosts.at(blkLocations, 0);
            return ImmutableList.of(new FileSplit(path, 0, length, splitHosts.getHosts(), splitHosts.getCachedHosts()));
        }

        ImmutableList.Builder<FileSplit> splits = ImmutableList.builder();
        long splitSize = fileStatus.getBlockSize();

        long bytesRemaining = length;
        while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
            SplitHosts splitHosts = SplitHosts.at(blkLocations, length - bytesRemaining);
            splits.add(new FileSplit(path, length - bytesRemaining, splitSize, splitHosts.getHosts(), splitHosts.getCachedHosts()));
            bytesRemaining -= splitSize;
        }
        if (bytesRemaining != 0) {
            SplitHosts splitHosts = SplitHosts.at(blkLocations, length - bytesRemaining);
            splits.add(new FileSplit(path, length - bytesRemaining, bytesRemaining, splitHosts.getHosts(), splitHosts.getCachedHosts()));
        }
        return splits.build();
    }

    private static boolean isSplitable(Path filename)
    {
        return !(filename instanceof PathWithBootstrapFileStatus);
    }

    private static int getBlockIndex(BlockLocation[] blkLocations, long offset)
    {
        for (int i = 0; i < blkLocations.length; i++) {
            if (isOffsetInBlock(blkLocations[i], offset)) {
                return i;
            }
        }
        BlockLocation last = blkLocations[blkLocations.length - 1];
        long fileLength = last.getOffset() + last.getLength() - 1;
        throw new IllegalArgumentException(format("Offset %d is outside of file (0..%d)", offset, fileLength));
    }

    private static boolean isOffsetInBlock(BlockLocation blkLocation, long offset)
    {
        return (blkLocation.getOffset() <= offset) &&
                (offset < blkLocation.getOffset() + blkLocation.getLength());
    }

    private static class SplitHosts
    {
        private final String[] hosts;
        private final String[] cachedHosts;

        private static SplitHosts at(BlockLocation[] blkLocations, long offset)
                throws IOException
        {
            int index = getBlockIndex(blkLocations, offset);
            return new SplitHosts(blkLocations[index].getHosts(), blkLocations[index].getCachedHosts());
        }

        private SplitHosts(String[] hosts, String[] cachedHosts)
        {
            this.hosts = hosts;
            this.cachedHosts = cachedHosts;
        }

        public String[] getHosts()
        {
            return hosts;
        }

        public String[] getCachedHosts()
        {
            return cachedHosts;
        }
    }
}
