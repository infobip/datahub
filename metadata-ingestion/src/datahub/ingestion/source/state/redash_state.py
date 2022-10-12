from typing import Dict, List, Set

from datahub.ingestion.source.state.checkpoint import CheckpointStateBase


class RedashCheckpointsList(CheckpointStateBase):
    _state_ids: List[int] = list()

    def get_ids(self) -> List[int]:
        return self._state_ids

    def add_id(self, checkpoint_id: int):
        self._state_ids.append(checkpoint_id)


class RedashCheckpointState(CheckpointStateBase):
    _workunits_hash_by_urn: Dict[str, Set[int]] = dict()

    def get_entries(self) -> Dict[str, Set[int]]:
        return self._workunits_hash_by_urn

    def add_entry(self, urn: str, wu_list: Set[int]):
        self._workunits_hash_by_urn[urn] = wu_list

    def size(self) -> int:
        return len(self._workunits_hash_by_urn)
