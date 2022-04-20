import React from 'react';
import {
    Container,
    Domain,
    EntityType,
    FabricType,
    GlobalTags,
    GlossaryTerms,
    Owner,
    SearchInsight,
} from '../../../../types.generated';
import DefaultPreviewCard from '../../../preview/DefaultPreviewCard';
import { useEntityRegistry } from '../../../useEntityRegistry';
import { capitalizeFirstLetterOnly } from '../../../shared/textUtil';
import { IconStyleType } from '../../Entity';

export const Preview = ({
    urn,
    name,
    origin,
    description,
    platformName,
    platformLogo,
    owners,
    globalTags,
    domain,
    snippet,
    insights,
    glossaryTerms,
    subtype,
    container,
}: {
    urn: string;
    name: string;
    origin: FabricType;
    description?: string | null;
    platformName: string;
    platformLogo?: string | null;
    owners?: Array<Owner> | null;
    domain?: Domain | null;
    globalTags?: GlobalTags | null;
    snippet?: React.ReactNode | null;
    insights?: Array<SearchInsight> | null;
    glossaryTerms?: GlossaryTerms | null;
    subtype?: string | null;
    container?: Container | null;
}): JSX.Element => {
    const entityRegistry = useEntityRegistry();
    const capitalPlatformName = capitalizeFirstLetterOnly(platformName);
    return (
        <DefaultPreviewCard
            url={entityRegistry.getEntityUrl(EntityType.SimilarityGroup, urn)}
            name={name || ''}
            description={description || ''}
            type={capitalizeFirstLetterOnly(subtype) || 'Similarity group'}
            logoUrl={platformLogo || ''}
            typeIcon={entityRegistry.getIcon(EntityType.SimilarityGroup, 12, IconStyleType.ACCENT)}
            platform={capitalPlatformName}
            qualifier={origin}
            tags={globalTags || undefined}
            owners={owners}
            domain={domain}
            container={container || undefined}
            snippet={snippet}
            insights={insights}
            glossaryTerms={glossaryTerms || undefined}
        />
    );
};
