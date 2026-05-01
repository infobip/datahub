from prometheus_client import Counter

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import WorkUnit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.mxe import (
    MetadataChangeEvent,
    MetadataChangeProposal,
)

ingestionEntitiesCounter = Counter(
    name="ingested_total",
    documentation="Ingested entities counts",
    labelnames=["work_unit", "entity_type", "change_type"],
)

ingestionIssuesCounter = Counter(
    name="ingestion_issues_total",
    documentation="Ingested issues counts",
    labelnames=["issue_type", "reason"],
)


def report_ingested_workunit_to_prometheus(wu: WorkUnit) -> None:
    if isinstance(wu, MetadataWorkUnit):
        if isinstance(wu.metadata, MetadataChangeEvent):
            # Avoid decompose_mce_into_mcps() here — the pipeline calls it separately
            # and doing it again per workunit just for counting is a significant overhead.
            ingestionEntitiesCounter.labels(
                work_unit=wu.__class__.__name__ + "." + wu.metadata.__class__.__name__,
                entity_type="mce",
                change_type="UPSERT",
            ).inc()
        elif isinstance(wu.metadata, MetadataChangeProposal) or isinstance(
            wu.metadata, MetadataChangeProposalWrapper
        ):
            ingestionEntitiesCounter.labels(
                work_unit=wu.__class__.__name__ + "." + wu.metadata.__class__.__name__,
                entity_type=wu.metadata.entityType,
                change_type=wu.metadata.changeType,
            ).inc()


def report_ingestion_issue_to_prometheus(issue_type: str, reason: str) -> None:
    ingestionIssuesCounter.labels(issue_type=issue_type, reason=reason).inc()
