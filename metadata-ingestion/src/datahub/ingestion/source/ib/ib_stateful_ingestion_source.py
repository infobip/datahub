import logging
from abc import abstractmethod
from typing import Iterable, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext, WorkUnit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.ib.utils.state_manager import StateManager
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.stateful_ingestion_base import (
    JobId,
    StatefulIngestionConfig,
    StatefulIngestionConfigBase,
    StatefulIngestionReport,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import Status
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import ChangeTypeClass, StatusClass
from datahub.utilities.urns.urn import Urn

logger = logging.getLogger(__name__)


class IBSourceStatefulIngestionConfig(StatefulIngestionConfig):
    remove_stale_metadata: bool = True


class IBSourceConfig(StatefulIngestionConfigBase):
    stateful_ingestion: Optional[IBSourceStatefulIngestionConfig] = None


class IBSourceReport(StatefulIngestionReport):
    events_skipped: int = 0
    events_deleted: int = 0


class IBStatefulIngestionSource(StatefulIngestionSourceBase):
    config: IBSourceConfig
    state_manager: StateManager
    report: IBSourceReport

    def __init__(self, config: IBSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config: IBSourceConfig = config
        self.report: IBSourceReport = IBSourceReport()
        self.state_manager = StateManager(
            config,
            ctx,
            self.get_default_ingestion_job_id_prefix(),
            self.get_last_checkpoint,
            self.get_current_checkpoint,
        )

    @abstractmethod
    def fetch_workunits(self) -> Iterable[WorkUnit]:
        raise NotImplementedError("Sub-classes must implement this method.")

    def get_workunits(self) -> Iterable[WorkUnit]:
        if not self.is_stateful_ingestion_configured():
            for wu in self.fetch_workunits():
                self.report.report_workunit(wu)
                yield wu
            return

        for wu in self.fetch_workunits():
            if type(wu) is not MetadataWorkUnit:
                yield wu
                continue

            if type(wu.metadata) is MetadataChangeEvent:
                urn = wu.metadata.proposedSnapshot.urn
                wu.metadata.proposedSnapshot.aspects.append(StatusClass(removed=False))
            elif type(wu.metadata) is MetadataChangeProposalWrapper:
                urn = wu.metadata.entityUrn
            else:
                raise TypeError(f"Unknown metadata type {type(wu.metadata)}")

            self.state_manager.current_add_workunit(urn, wu)

            # Emitting workuntis not presented in last state
            if not self.state_manager.last_has_workunit(urn, wu):
                self.report.report_workunit(wu)
                yield wu
                continue
            else:
                self.report.events_skipped += 1

        if (
            self.config.stateful_ingestion
            and self.config.stateful_ingestion.remove_stale_metadata
        ):
            # Deleting workunits not presented in current state
            for urn_str in self.state_manager.get_deleted_urns():
                urn = Urn.create_from_string(urn_str)
                mcp = MetadataChangeProposalWrapper(
                    entityType=urn.get_type(),
                    entityUrn=urn_str,
                    changeType=ChangeTypeClass.UPSERT,
                    aspectName="status",
                    aspect=Status(removed=True),
                )
                self.report.events_deleted += 1
                yield MetadataWorkUnit(id=f"soft-delete-{urn_str}", mcp=mcp)

        self.state_manager.save_state()

    def close(self):
        self.prepare_for_commit()

    def get_report(self) -> IBSourceReport:
        return self.report

    def is_checkpointing_enabled(self, job_id: JobId) -> bool:
        if (
            job_id.startswith(self.get_default_ingestion_job_id_prefix())
            and self.is_stateful_ingestion_configured()
            and self.config.stateful_ingestion
            and self.config.stateful_ingestion.remove_stale_metadata
        ):
            return True

        return False

    def create_checkpoint(self, job_id: JobId) -> Optional[Checkpoint]:
        return self.state_manager.create_checkpoint(job_id)

    @abstractmethod
    def get_default_ingestion_job_id_prefix(self) -> JobId:
        raise NotImplementedError("Sub-classes must implement this method.")
