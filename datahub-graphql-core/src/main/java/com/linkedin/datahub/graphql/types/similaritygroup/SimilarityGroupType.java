package com.linkedin.datahub.graphql.types.similaritygroup;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AutoCompleteResults;
import com.linkedin.datahub.graphql.generated.BrowsePath;
import com.linkedin.datahub.graphql.generated.BrowseResults;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.generated.SimilarityGroup;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.types.BrowsableEntityType;
import com.linkedin.datahub.graphql.types.SearchableEntityType;
import com.linkedin.datahub.graphql.types.mappers.AutoCompleteResultsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowsePathsMapper;
import com.linkedin.datahub.graphql.types.mappers.BrowseResultMapper;
import com.linkedin.datahub.graphql.types.mappers.UrnSearchResultsMapper;
import com.linkedin.datahub.graphql.types.similaritygroup.mappers.SimilarityGroupMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.browse.BrowseResult;
import com.linkedin.metadata.query.AutoCompleteResult;
import com.linkedin.metadata.search.SearchResult;
import graphql.execution.DataFetcherResult;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.linkedin.datahub.graphql.Constants.BROWSE_PATH_DELIMITER;
import static com.linkedin.metadata.Constants.*;


public class SimilarityGroupType implements SearchableEntityType<SimilarityGroup>, BrowsableEntityType<SimilarityGroup> {

    private static final Set<String> ASPECTS_TO_RESOLVE = ImmutableSet.of(
            SIMILARITY_GROUP_KEY_ASPECT_NAME,
            SIMILARITY_GROUP_PROPERTIES_ASPECT_NAME,
            OWNERSHIP_ASPECT_NAME
    );

    private static final Set<String> FACET_FIELDS = ImmutableSet.of("origin", "platform");
    private static final String ENTITY_NAME = "similarityGroup";

    private final EntityClient _entityClient;

    public SimilarityGroupType(final EntityClient entityClient) {
        _entityClient = entityClient;
    }

    @Override
    public Class<SimilarityGroup> objectClass() {
        return SimilarityGroup.class;
    }

    @Override
    public EntityType type() {
        return EntityType.SIMILARITY_GROUP;
    }

    @Override
    public List<DataFetcherResult<SimilarityGroup>> batchLoad(final List<String> urnStrs, final QueryContext context) {
        final List<Urn> urns = urnStrs.stream()
                .map(UrnUtils::getUrn)
                .collect(Collectors.toList());
        try {
            final Map<Urn, EntityResponse> similarityGroupMap =
                    _entityClient.batchGetV2(
                            Constants.SIMILARITY_GROUP_ENTITY_NAME,
                            new HashSet<>(urns),
                            ASPECTS_TO_RESOLVE,
                            context.getAuthentication());

            final List<EntityResponse> gmsResults = new ArrayList<>();
            for (Urn urn : urns) {
                gmsResults.add(similarityGroupMap.getOrDefault(urn, null));
            }
            return gmsResults.stream()
                    .map(gmsSimilarityGroup -> gmsSimilarityGroup == null ? null : DataFetcherResult.<SimilarityGroup>newResult()
                            .data(SimilarityGroupMapper.map(gmsSimilarityGroup))
                            .build())
                    .collect(Collectors.toList());
        } catch (Exception e) {
            throw new RuntimeException("Failed to batch load Datasets", e);
        }
    }

    @Override
    public BrowseResults browse(@Nonnull List<String> path,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull final QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final String pathStr = path.size() > 0 ? BROWSE_PATH_DELIMITER + String.join(BROWSE_PATH_DELIMITER, path) : "";
        final BrowseResult result = _entityClient.browse(
                "similarityGroup",
                pathStr,
                facetFilters,
                start,
                count,
                context.getAuthentication());
        return BrowseResultMapper.map(result);
    }

    @Override
    public List<BrowsePath> browsePaths(@Nonnull String urn, @Nonnull final QueryContext context) throws Exception {
        final StringArray result = _entityClient.getBrowsePaths(SimilarityGroupUtils.getSimilarityGroupUrn(urn), context.getAuthentication());
        return BrowsePathsMapper.map(result);
    }

    @Override
    public SearchResults search(@Nonnull String query,
                                @Nullable List<FacetFilterInput> filters,
                                int start,
                                int count,
                                @Nonnull QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final SearchResult searchResult = _entityClient.search(ENTITY_NAME, query, facetFilters, start, count, context.getAuthentication());
        return UrnSearchResultsMapper.map(searchResult);
    }

    @Override
    public AutoCompleteResults autoComplete(@Nonnull String query,
                                            @Nullable String field,
                                            @Nullable List<FacetFilterInput> filters,
                                            int limit,
                                            @Nonnull QueryContext context) throws Exception {
        final Map<String, String> facetFilters = ResolverUtils.buildFacetFilters(filters, FACET_FIELDS);
        final AutoCompleteResult result = _entityClient.autoComplete(ENTITY_NAME, query, facetFilters, limit, context.getAuthentication());
        return AutoCompleteResultsMapper.map(result);
    }
}
