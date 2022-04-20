package com.linkedin.datahub.graphql.types.similaritygroup.mappers;

import com.linkedin.common.Ownership;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.generated.DataPlatform;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FabricType;
import com.linkedin.datahub.graphql.generated.SimilarityGroup;
import com.linkedin.datahub.graphql.types.common.mappers.OwnershipMapper;
import com.linkedin.datahub.graphql.types.common.mappers.StringMapMapper;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.datahub.graphql.types.mappers.ModelMapper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.SimilarityGroupKey;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;

import static com.linkedin.metadata.Constants.*;


/**
 * Maps GMS response objects to objects conforming to the GQL schema.
 *
 * To be replaced by auto-generated mappers implementations
 */
@Slf4j
public class SimilarityGroupMapper implements ModelMapper<EntityResponse, SimilarityGroup> {

    public static final SimilarityGroupMapper INSTANCE = new SimilarityGroupMapper();

    public static SimilarityGroup map(@Nonnull final EntityResponse dataset) {
        return INSTANCE.apply(dataset);
    }

    @Override
    public SimilarityGroup apply(@Nonnull final EntityResponse entityResponse) {
        SimilarityGroup result = new SimilarityGroup();
        result.setUrn(entityResponse.getUrn().toString());
        result.setType(EntityType.SIMILARITY_GROUP);

        EnvelopedAspectMap aspectMap = entityResponse.getAspects();
        MappingHelper<SimilarityGroup> mappingHelper = new MappingHelper<>(aspectMap, result);
        mappingHelper.mapToResult(SIMILARITY_GROUP_KEY_ASPECT_NAME, this::mapSimilarityGroupKey);
        mappingHelper.mapToResult(SIMILARITY_GROUP_PROPERTIES_ASPECT_NAME, this::mapSimilarityGroupProperties);
        mappingHelper.mapToResult(OWNERSHIP_ASPECT_NAME, (dataset, dataMap) ->
                dataset.setOwnership(OwnershipMapper.map(new Ownership(dataMap))));

        return mappingHelper.getResult();
    }

    private void mapSimilarityGroupKey(@Nonnull SimilarityGroup similarityGroup, @Nonnull DataMap dataMap) {
        final SimilarityGroupKey gmsKey = new SimilarityGroupKey(dataMap);
        similarityGroup.setName(gmsKey.getName());
        similarityGroup.setOrigin(FabricType.valueOf(gmsKey.getOrigin().toString()));
        similarityGroup.setPlatform(DataPlatform.builder()
                .setType(EntityType.DATA_PLATFORM)
                .setUrn(gmsKey.getPlatform().toString()).build());
    }

    private void mapSimilarityGroupProperties(@Nonnull SimilarityGroup similarityGroup, @Nonnull DataMap dataMap) {
        final com.linkedin.similaritygroup.SimilarityGroupProperties gmsProperties = new com.linkedin.similaritygroup.SimilarityGroupProperties(dataMap);
        final com.linkedin.datahub.graphql.generated.SimilarityGroupProperties properties =
                new com.linkedin.datahub.graphql.generated.SimilarityGroupProperties();
        properties.setDescription(gmsProperties.getDescription());
        properties.setOrigin(similarityGroup.getOrigin());
        properties.setCustomProperties(StringMapMapper.map(gmsProperties.getCustomProperties()));
        properties.setName(similarityGroup.getName());
        similarityGroup.setProperties(properties);
        similarityGroup.setDescription(properties.getDescription());
    }
}
