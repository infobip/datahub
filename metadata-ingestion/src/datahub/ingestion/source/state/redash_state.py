import zlib
from typing import Dict, Iterable, Set

import pickle

from datahub.ingestion.api.common import WorkUnit
from datahub.ingestion.source.state.checkpoint import CheckpointStateBase


class RedashCheckpointState(CheckpointStateBase):

    _workunits_hash_by_urn: Dict[str, Set[int]] = dict()

    def get_urns_not_in(self, checkpoint: "RedashCheckpointState") -> Iterable[str]:
        return self._workunits_hash_by_urn.keys() - checkpoint._workunits_hash_by_urn.keys()

    def has_workunit(self, urn: str, wu: WorkUnit):
        workunit_hash = RedashCheckpointState._hash_workunit(wu)
        return workunit_hash in self._workunits_hash_by_urn.get(urn, set())

    def add_workunit(self, urn: str, wu: WorkUnit) -> None:
        workunits = self._workunits_hash_by_urn.get(urn, set())
        workunits.add(RedashCheckpointState._hash_workunit(wu))
        self._workunits_hash_by_urn[urn] = workunits

    def get_workunits(self) -> Dict[str, Set[int]]:
        return self._workunits_hash_by_urn

    @staticmethod
    def _hash_workunit(wu: WorkUnit) -> int:
        return zlib.crc32(pickle.dumps(wu)) & 0xFFFFFFFF
