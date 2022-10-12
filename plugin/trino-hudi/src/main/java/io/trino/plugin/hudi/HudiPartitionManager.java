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

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HiveBucketHandle;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HivePartition;
import io.trino.plugin.hive.HivePartitionManager;
import io.trino.plugin.hive.HivePartitionResult;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.util.HiveBucketing;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.metastore.MetastoreUtil.computePartitionKeyFilter;
import static io.trino.plugin.hive.util.HiveBucketing.getHiveBucketFilter;
import static java.util.stream.Collectors.toList;

public class HudiPartitionManager extends HivePartitionManager {
    private final int maxPartitions;
    private final int domainCompactionThreshold;

  public HudiPartitionManager(HiveConfig hiveConfig) {
    super(hiveConfig);
    this.maxPartitions = hiveConfig.getMaxPartitionsPerScan();
    this.domainCompactionThreshold = hiveConfig.getDomainCompactionThreshold();
  }

  public HivePartitionResult getPartitions(HiveMetastore metastore, ConnectorTableHandle tableHandle, Constraint constraint)
  {
    HiveTableHandle hiveTableHandle = (HiveTableHandle) tableHandle;
    TupleDomain<ColumnHandle> effectivePredicate = constraint.getSummary()
        .intersect(hiveTableHandle.getEnforcedConstraint());

    SchemaTableName tableName = hiveTableHandle.getSchemaTableName();
    Optional<HiveBucketHandle> hiveBucketHandle = hiveTableHandle.getBucketHandle();
    List<HiveColumnHandle> partitionColumns = hiveTableHandle.getPartitionColumns();

    if (effectivePredicate.isNone()) {
      return new HivePartitionResult(partitionColumns, Optional.empty(), ImmutableList.of(), TupleDomain.none(), TupleDomain.none(), hiveBucketHandle, Optional.empty());
    }

    Optional<HiveBucketing.HiveBucketFilter> bucketFilter = getHiveBucketFilter(hiveTableHandle, effectivePredicate);
    TupleDomain<HiveColumnHandle> compactEffectivePredicate = effectivePredicate
        .transformKeys(HiveColumnHandle.class::cast)
        .simplify(domainCompactionThreshold);

    if (partitionColumns.isEmpty()) {
      return new HivePartitionResult(
          partitionColumns,
          Optional.empty(),
          ImmutableList.of(new HivePartition(tableName)),
          effectivePredicate,
          compactEffectivePredicate,
          hiveBucketHandle,
          bucketFilter);
    }

    List<Type> partitionTypes = partitionColumns.stream()
        .map(HiveColumnHandle::getType)
        .collect(toList());

    Optional<List<String>> partitionNames = Optional.empty();
    Iterable<HivePartition> partitionsIterable;
    Predicate<Map<ColumnHandle, NullableValue>> predicate = constraint.predicate().orElse(value -> true);
    if (hiveTableHandle.getPartitions().isPresent()) {
      partitionsIterable = hiveTableHandle.getPartitions().get().stream()
          .filter(partition -> partitionMatches(partitionColumns, effectivePredicate, predicate, partition))
          .collect(toImmutableList());
    }
    else {
      List<String> partitionNamesList = hiveTableHandle.getPartitionNames()
          .orElseGet(() -> getFilteredPartitionNames(metastore, tableName, partitionColumns, compactEffectivePredicate));
      partitionsIterable = () -> partitionNamesList.stream()
          // Apply extra filters which could not be done by getFilteredPartitionNames
          .map(partitionName -> parseValuesAndFilterPartition(tableName, partitionName, partitionColumns, partitionTypes, effectivePredicate, predicate))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .iterator();
      partitionNames = Optional.of(partitionNamesList);
    }

    return new HivePartitionResult(partitionColumns, partitionNames, partitionsIterable, effectivePredicate, compactEffectivePredicate, hiveBucketHandle, bucketFilter);
  }

  private boolean partitionMatches(List<HiveColumnHandle> partitionColumns, TupleDomain<ColumnHandle> constraintSummary, Predicate<Map<ColumnHandle, NullableValue>> constraint, HivePartition partition)
  {
    return partitionMatches(partitionColumns, constraintSummary, partition) && constraint.test(partition.getKeys());
  }

  private List<String> getFilteredPartitionNames(HiveMetastore metastore, SchemaTableName tableName, List<HiveColumnHandle> partitionKeys, TupleDomain<HiveColumnHandle> effectivePredicate)
  {
    List<String> columnNames = partitionKeys.stream()
        .map(HiveColumnHandle::getName)
        .collect(toImmutableList());
    TupleDomain<String> partitionKeysFilter = computePartitionKeyFilter(partitionKeys, effectivePredicate);
    // fetch the partition names
    return metastore.getPartitionNamesByFilter(tableName.getSchemaName(), tableName.getTableName(), columnNames, partitionKeysFilter)
        .orElseThrow(() -> new TableNotFoundException(tableName));
  }

  private Optional<HivePartition> parseValuesAndFilterPartition(
      SchemaTableName tableName,
      String partitionId,
      List<HiveColumnHandle> partitionColumns,
      List<Type> partitionColumnTypes,
      TupleDomain<ColumnHandle> constraintSummary,
      Predicate<Map<ColumnHandle, NullableValue>> constraint)
  {
    HivePartition partition = parsePartition(tableName, partitionId, partitionColumns, partitionColumnTypes);

    if (partitionMatches(partitionColumns, constraintSummary, constraint, partition)) {
      return Optional.of(partition);
    }
    return Optional.empty();
  }
}
