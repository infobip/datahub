from typing import Callable, Optional

from datahub.ingestion.source.state.checkpoint import Checkpoint


class StateManager:
    _state_prefix: str

    _last_states_factory: Callable[[str], Optional[Checkpoint]]
    _current_states_factory: Callable[[str], Optional[Checkpoint]]
