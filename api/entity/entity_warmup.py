from dataclasses import dataclass, field, is_dataclass
from typing import Dict, Any, List


@dataclass
class SnapshotComponent:
    metadata: Dict[str, Any] = field(default_factory=dict)
    trace: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class Snapshot:
    file: SnapshotComponent = field(default_factory=SnapshotComponent)
    db: SnapshotComponent = field(default_factory=SnapshotComponent)
    batch: SnapshotComponent = field(default_factory=SnapshotComponent)


@dataclass
class WarmupData:
    version: str
    snapshot: Snapshot = field(default_factory=Snapshot)
