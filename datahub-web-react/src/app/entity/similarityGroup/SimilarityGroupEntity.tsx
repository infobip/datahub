import * as React from 'react';
import { DatabaseFilled, DatabaseOutlined } from '@ant-design/icons';
import { Typography } from 'antd';
import { SimilarityGroup, EntityType, SearchResult } from '../../../types.generated';
import { Entity, IconStyleType, PreviewType } from '../Entity';
import { GetSimilarityGroupQuery, useGetSimilarityGroupQuery } from '../../../graphql/similaritygroup.generated';
import { PropertiesTab } from '../shared/tabs/Properties/PropertiesTab';
import { SidebarOwnerSection } from '../shared/containers/profile/sidebar/Ownership/SidebarOwnerSection';
import { LineageTab } from '../shared/tabs/Lineage/LineageTab';
import { EntityProfile } from '../shared/containers/profile/EntityProfile';
import { getDataForEntityType } from '../shared/containers/profile/utils';
import { Preview } from './preview/Preview';
import { FIELDS_TO_HIGHLIGHT } from './search/highlights';
import { EntityAndType } from '../../lineage/types';

/**
 * Definition of the DataHub Dataset entity.
 */
export class SimilarityGroupEntity implements Entity<SimilarityGroup> {
    type: EntityType = EntityType.SimilarityGroup;

    icon = (fontSize: number, styleType: IconStyleType) => {
        if (styleType === IconStyleType.TAB_VIEW) {
            return <DatabaseOutlined style={{ fontSize }} />;
        }

        if (styleType === IconStyleType.HIGHLIGHT) {
            return <DatabaseFilled style={{ fontSize, color: '#B37FEB' }} />;
        }

        if (styleType === IconStyleType.SVG) {
            return (
                <path d="M832 64H192c-17.7 0-32 14.3-32 32v832c0 17.7 14.3 32 32 32h640c17.7 0 32-14.3 32-32V96c0-17.7-14.3-32-32-32zm-600 72h560v208H232V136zm560 480H232V408h560v208zm0 272H232V680h560v208zM304 240a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0zm0 272a40 40 0 1080 0 40 40 0 10-80 0z" />
            );
        }

        return (
            <DatabaseOutlined
                style={{
                    fontSize,
                    color: '#BFBFBF',
                }}
            />
        );
    };

    isSearchEnabled = () => true;

    isBrowseEnabled = () => true;

    isLineageEnabled = () => true;

    getAutoCompleteFieldName = () => 'name';

    getPathName = () => 'similarityGroup';

    getEntityName = () => 'Similarity Group';

    getCollectionName = () => 'Similarity Groups';

    renderProfile = (urn: string) => (
        <EntityProfile
            urn={urn}
            entityType={EntityType.SimilarityGroup}
            useEntityQuery={useGetSimilarityGroupQuery}
            getOverrideProperties={() => {
                return {};
            }}
            tabs={[
                {
                    name: 'Properties',
                    component: PropertiesTab,
                },
                {
                    name: 'Lineage',
                    component: LineageTab,
                    display: {
                        visible: (_, _1) => true,
                        enabled: (_, similarityGroup: GetSimilarityGroupQuery) => {
                            return (
                                (similarityGroup?.similarityGroup?.upstream?.total || 0) > 0 ||
                                (similarityGroup?.similarityGroup?.downstream?.total || 0) > 0
                            );
                        },
                    },
                },
            ]}
            sidebarSections={[
                {
                    component: SidebarOwnerSection,
                },
            ]}
        />
    );

    renderPreview = (_: PreviewType, data: SimilarityGroup) => {
        return (
            <Preview
                urn={data.urn}
                name={data.properties?.name || data.name}
                origin={data.origin}
                description={data.properties?.description}
                platformName={data.platform.properties?.displayName || data.platform.name}
                platformLogo={data.platform.properties?.logoUrl}
                owners={data.ownership?.owners}
            />
        );
    };

    renderSearch = (result: SearchResult) => {
        const data = result.entity as SimilarityGroup;
        return (
            <Preview
                urn={data.urn}
                name={data.properties?.name || data.name}
                origin={data.origin}
                description={data.properties?.description}
                platformName={data.platform.properties?.displayName || data.platform.name}
                platformLogo={data.platform.properties?.logoUrl}
                owners={data.ownership?.owners}
                snippet={
                    // Add match highlights only if all the matched fields are in the FIELDS_TO_HIGHLIGHT
                    result.matchedFields.length > 0 &&
                    result.matchedFields.every((field) => FIELDS_TO_HIGHLIGHT.has(field.name)) && (
                        <Typography.Text>
                            Matches {FIELDS_TO_HIGHLIGHT.get(result.matchedFields[0].name)}{' '}
                            <b>{result.matchedFields[0].value}</b>
                        </Typography.Text>
                    )
                }
                insights={result.insights}
            />
        );
    };

    getLineageVizConfig = (entity: SimilarityGroup) => {
        return {
            urn: entity?.urn,
            name: entity.properties?.name || entity.name,
            type: EntityType.SimilarityGroup,
            // eslint-disable-next-line @typescript-eslint/dot-notation
            downstreamChildren: entity?.['downstream']?.relationships?.map(
                (relationship) => ({ entity: relationship.entity, type: relationship.entity.type } as EntityAndType),
            ),
            // eslint-disable-next-line @typescript-eslint/dot-notation
            upstreamChildren: entity?.['upstream']?.relationships?.map(
                (relationship) => ({ entity: relationship.entity, type: relationship.entity.type } as EntityAndType),
            ),
            icon: entity?.platform?.properties?.logoUrl || undefined,
            platform: entity?.platform?.name,
        };
    };

    displayName = (data: SimilarityGroup) => {
        return data?.properties?.name || data.name;
    };

    platformLogoUrl = (data: SimilarityGroup) => {
        return data.platform.properties?.logoUrl || undefined;
    };

    getGenericEntityProperties = (data: SimilarityGroup) => {
        return getDataForEntityType({
            data,
            entityType: this.type,
            getOverrideProperties: () => {
                return {};
            },
        });
    };
}
