package io.trino.plugin.hudi.query.index;

import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.util.Map;
import java.util.Optional;

public class IndexSupportFactory
{
    public static Map<String, HoodieIndexDefinition> getIndexDefinitions(HoodieTableMetaClient metaClient)
    {
        if (metaClient.getIndexMetadata().isEmpty()) {
            return Map.of();
        }

        Map<String, HoodieIndexDefinition> indexDefinitions = metaClient.getIndexMetadata().get().getIndexDefinitions();
        return indexDefinitions;
    }

    public static Optional<HudiIndexSupport> createIndexDefinition(
            HoodieTableMetaClient metaClient,
            TupleDomain<String> tupleDomain)
    {
        if (HudiColumnStatsIndexSupport.shouldUseIndex(metaClient, tupleDomain)) {
            return Optional.of(new HudiColumnStatsIndexSupport(metaClient));
        }
        else {
            return Optional.empty();
        }
    }
}