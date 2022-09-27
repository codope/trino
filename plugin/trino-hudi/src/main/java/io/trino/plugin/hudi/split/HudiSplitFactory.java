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
import io.trino.filesystem.FileEntry;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.plugin.hudi.files.FileSlice;
import io.trino.plugin.hudi.files.HudiBaseFile;
import io.trino.plugin.hudi.files.HudiLogFile;
import io.trino.plugin.hudi.model.HudiTableType;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_UNSUPPORTED_TABLE_TYPE;
import static io.trino.plugin.hudi.model.HudiTableType.COPY_ON_WRITE;
import static io.trino.plugin.hudi.model.HudiTableType.MERGE_ON_READ;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HudiSplitFactory
{
    private static final double SPLIT_SLOP = 1.1;   // 10% slop/overflow allowed in bytes per split while generating splits

    private final HudiTableHandle hudiTableHandle;
    private final HudiSplitWeightProvider hudiSplitWeightProvider;

    public HudiSplitFactory(
            HudiTableHandle hudiTableHandle,
            HudiSplitWeightProvider hudiSplitWeightProvider)
    {
        this.hudiTableHandle = requireNonNull(hudiTableHandle, "hudiTableHandle is null");
        this.hudiSplitWeightProvider = requireNonNull(hudiSplitWeightProvider, "hudiSplitWeightProvider is null");
    }

    public List<HudiSplit> createSplits(List<HivePartitionKey> partitionKeys, FileSlice fileSlice, String commitTime)
    {
        HudiTableType tableType = hudiTableHandle.getTableType();
        switch (tableType) {
            case COPY_ON_WRITE:
                return createSplitsForCOW(partitionKeys, fileSlice, commitTime);
            case MERGE_ON_READ:
                return createSplitsForMOR(partitionKeys, fileSlice, commitTime);
            default:
                throw new TrinoException(HUDI_UNSUPPORTED_TABLE_TYPE, format("Unsupported table type: %s", tableType));
        }
    }

    private List<HudiSplit> createSplitsForCOW(List<HivePartitionKey> partitionKeys, FileSlice fileSlice, String commitTime)
    {
        if (!fileSlice.getBaseFile().isPresent()) {
            return ImmutableList.of();
        }

        HudiBaseFile baseFile = fileSlice.getBaseFile().get();
        long fileSize = baseFile.getFileSize();

        if (fileSize == 0) {
            return ImmutableList.of(new HudiSplit(
                    ImmutableList.of(),
                    hudiTableHandle.getRegularPredicates(),
                    partitionKeys,
                    hudiSplitWeightProvider.calculateSplitWeight(fileSize),
                    fileSlice.getBaseFile(),
                    ImmutableList.of(),
                    commitTime));
        }

        ImmutableList.Builder<HudiSplit> splits = ImmutableList.builder();
        FileEntry baseFileEntry = baseFile.getFileEntry();
        long splitSize = baseFileEntry.blocks()
                .map(listOfBlocks -> (!listOfBlocks.isEmpty()) ? listOfBlocks.get(0).length() : 0).orElse(0L);

        long bytesRemaining = fileSize;
        while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
            FileEntry fileEntry = new FileEntry(
                    baseFileEntry.location(),
                    fileSize,
                    baseFileEntry.lastModified(),
                    Optional.of(ImmutableList.of(new FileEntry.Block(
                            ImmutableList.of(),
                            fileSize - bytesRemaining,
                            splitSize))));

            splits.add(new HudiSplit(
                    ImmutableList.of(),
                    hudiTableHandle.getRegularPredicates(),
                    partitionKeys,
                    hudiSplitWeightProvider.calculateSplitWeight(splitSize),
                    Optional.of(new HudiBaseFile(fileEntry)),
                    ImmutableList.of(),
                    commitTime));
            bytesRemaining -= splitSize;
        }
        if (bytesRemaining > 0) {
            FileEntry fileEntry = new FileEntry(
                    baseFileEntry.location(),
                    fileSize,
                    baseFileEntry.lastModified(),
                    Optional.of(ImmutableList.of(new FileEntry.Block(
                            ImmutableList.of(),
                            fileSize - bytesRemaining,
                            bytesRemaining))));

            splits.add(new HudiSplit(
                    ImmutableList.of(),
                    hudiTableHandle.getRegularPredicates(),
                    partitionKeys,
                    hudiSplitWeightProvider.calculateSplitWeight(bytesRemaining),
                    Optional.of(new HudiBaseFile(fileEntry)),
                    ImmutableList.of(),
                    commitTime));
        }
        return splits.build();
    }

    private List<HudiSplit> createSplitsForMOR(List<HivePartitionKey> partitionKeys, FileSlice fileSlice, String commitTime)
    {
        Optional<HudiBaseFile> baseFile = fileSlice.getBaseFile();
        List<HudiLogFile> logFiles = fileSlice.getLogFiles().stream().collect(toImmutableList());

        long logFilesSize = logFiles.size() > 0 ? logFiles.stream().map(HudiLogFile::getFileSize).reduce(0L, Long::sum) : 0L;
        long fileSize = baseFile.isPresent() ? baseFile.get().getFileEntry().length() + logFilesSize : logFilesSize;

        HudiSplit split = new HudiSplit(
                ImmutableList.of(),
                hudiTableHandle.getRegularPredicates(),
                partitionKeys,
                hudiSplitWeightProvider.calculateSplitWeight(fileSize),
                baseFile,
                logFiles,
                commitTime);

        return ImmutableList.of(split);
    }
}
