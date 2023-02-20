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
import io.trino.plugin.hudi.HudiFile;
import io.trino.plugin.hudi.HudiFileStatus;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.HudiTableHandle;
import io.trino.spi.TrinoException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.hadoop.PathWithBootstrapFileStatus;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_FILESYSTEM_ERROR;
import static io.trino.plugin.hudi.model.HoodieTableType.COPY_ON_WRITE;
import static io.trino.plugin.hudi.model.HoodieTableType.MERGE_ON_READ;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.getFileStatus;

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

    public Stream<HudiSplit> createSplits(List<HivePartitionKey> partitionKeys, FileSlice fileSlice, String commitTime)
    {
        HudiFile baseFile = fileSlice.getBaseFile().map(f -> new HudiFile(f.getPath(), 0, f.getFileLen(), f.getFileSize(), f.getFileStatus().getModificationTime())).orElse(null);
        if (COPY_ON_WRITE.equals(hudiTableHandle.getTableType())) {
            if (baseFile == null) {
                return Stream.empty();
            }

            return createBaseFileSplits(partitionKeys, fileSlice, commitTime).stream();
        }
        else if (MERGE_ON_READ.equals(hudiTableHandle.getTableType())) {
            List<HudiFile> logFiles = fileSlice.getLogFiles()
                    .map(logFile -> new HudiFile(logFile.getPath().toString(), 0, logFile.getFileSize(), logFile.getFileSize(), logFile.getFileStatus().getModificationTime()))
                    .collect(toImmutableList());
            long logFilesSize = logFiles.size() > 0 ? logFiles.stream().map(HudiFile::getLength).reduce(0L, Long::sum) : 0L;
            long sizeInBytes = baseFile != null ? baseFile.getLength() + logFilesSize : logFilesSize;

            return Stream.of(new HudiSplit(
                    ImmutableList.of(),
                    hudiTableHandle.getRegularPredicates(),
                    partitionKeys,
                    hudiSplitWeightProvider.calculateSplitWeight(sizeInBytes),
                    Optional.ofNullable(baseFile),
                    logFiles,
                    commitTime));
        }
        else {
            throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, "Could not create page source for table type " + hudiTableHandle.getTableType());
        }
    }

    public List<HudiSplit> createBaseFileSplits(List<HivePartitionKey> partitionKeys, FileSlice fileSlice, String commitTime)
    {
        HudiFileStatus fileStatus;
        try {
            FileStatus status = getFileStatus(fileSlice.getBaseFile().get());
            fileStatus = new HudiFileStatus(
                    status.getPath(),
                    false,
                    status.getLen(),
                    status.getModificationTime(),
                    status.getBlockSize());
        }
        catch (IOException e) {
            throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, "Error getting file status of " + fileSlice.getBaseFile().get().getPath(), e);
        }
        if (fileStatus.isDirectory()) {
            throw new TrinoException(HUDI_FILESYSTEM_ERROR, format("Not a valid path: %s", fileStatus.path()));
        }

        long fileSize = fileStatus.length();

        if (fileSize == 0 || fileStatus.path() instanceof PathWithBootstrapFileStatus) {
            return ImmutableList.of(new HudiSplit(
                    ImmutableList.of(),
                    hudiTableHandle.getRegularPredicates(),
                    partitionKeys,
                    hudiSplitWeightProvider.calculateSplitWeight(fileSize),
                    Optional.of(new HudiFile(fileStatus.path().toString(), 0, fileSize, fileSize, fileStatus.modificationTime())),
                    ImmutableList.of(),
                    commitTime));
        }

        ImmutableList.Builder<HudiSplit> splits = ImmutableList.builder();
        long splitSize = fileStatus.blockSize();

        long bytesRemaining = fileSize;
        while (((double) bytesRemaining) / splitSize > SPLIT_SLOP) {
            splits.add(new HudiSplit(
                    ImmutableList.of(),
                    hudiTableHandle.getRegularPredicates(),
                    partitionKeys,
                    hudiSplitWeightProvider.calculateSplitWeight(fileSize),
                    Optional.of(new HudiFile(fileStatus.path().toString(), fileSize - bytesRemaining, splitSize, fileStatus.length(), fileStatus.modificationTime())),
                    ImmutableList.of(),
                    commitTime));
            bytesRemaining -= splitSize;
        }
        if (bytesRemaining > 0) {
            splits.add(new HudiSplit(
                    ImmutableList.of(),
                    hudiTableHandle.getRegularPredicates(),
                    partitionKeys,
                    hudiSplitWeightProvider.calculateSplitWeight(fileSize),
                    Optional.of(new HudiFile(fileStatus.path().toString(), fileSize - bytesRemaining, bytesRemaining, fileStatus.length(), fileStatus.modificationTime())),
                    ImmutableList.of(),
                    commitTime));
        }
        return splits.build();
    }
}
