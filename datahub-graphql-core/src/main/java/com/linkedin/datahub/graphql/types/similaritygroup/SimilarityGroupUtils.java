package com.linkedin.datahub.graphql.types.similaritygroup;

import com.linkedin.common.urn.SimilarityGroupUrn;

import java.net.URISyntaxException;

public class SimilarityGroupUtils {

    private SimilarityGroupUtils() { }

    static SimilarityGroupUrn getSimilarityGroupUrn(String urnStr) {
        try {
            return SimilarityGroupUrn.createFromString(urnStr);
        } catch (URISyntaxException e) {
            throw new RuntimeException(String.format("Failed to retrieve similarity group with urn %s, invalid urn", urnStr));
        }
    }
}
