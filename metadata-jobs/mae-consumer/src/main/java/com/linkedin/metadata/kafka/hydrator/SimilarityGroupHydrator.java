package com.linkedin.metadata.kafka.hydrator;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.data.DataMap;
import com.linkedin.datahub.graphql.types.common.mappers.util.MappingHelper;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.key.SimilarityGroupKey;
import lombok.extern.slf4j.Slf4j;

import static com.linkedin.metadata.Constants.SIMILARITY_GROUP_KEY_ASPECT_NAME;


@Slf4j
public class SimilarityGroupHydrator extends BaseHydrator {

    private static final String PLATFORM = "platform";
    private static final String NAME = "name";

    @Override
    protected void hydrateFromEntityResponse(ObjectNode document, EntityResponse entityResponse) {
        EnvelopedAspectMap aspectMap = entityResponse.getAspects();
        MappingHelper<ObjectNode> mappingHelper = new MappingHelper<>(aspectMap, document);
        mappingHelper.mapToResult(SIMILARITY_GROUP_KEY_ASPECT_NAME, this::mapKey);
    }

    private void mapKey(ObjectNode jsonNodes, DataMap dataMap) {
        SimilarityGroupKey similarityGroupKey = new SimilarityGroupKey(dataMap);
        jsonNodes.put(PLATFORM, similarityGroupKey.getPlatform().toString());
        jsonNodes.put(NAME, similarityGroupKey.getName());
    }
}
