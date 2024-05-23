from dataclasses import dataclass, field
from typing import List, Dict, Any


@dataclass
class SnapshotComponent:
    metadata: Dict[str, Any] = field(default_factory=dict)
    trace: List[Any] = field(default_factory=list)


@dataclass
class Snapshot:
    file: SnapshotComponent = field(default_factory=SnapshotComponent)
    db: SnapshotComponent = field(default_factory=SnapshotComponent)
    batch: SnapshotComponent = field(default_factory=SnapshotComponent)


@dataclass
class WarmupData:
    version: str
    snapshot: Snapshot = field(default_factory=Snapshot)


# 将读取的数据转换为dataclass对象
def mask(v, d):  # v 是 dict 数据, d 是 @dataclass 类型
    if isinstance(v, d):
        pass
    elif isinstance(v, Dict):
        x = [v.get(k, None) for k in [i for i in d.__dict__['__annotations__']]]
        y = list(d.__init__.__defaults__)
        if len(y) > 0:
            n = len(x)
            m = len(y)
            for i in range(m):
                if x[n - m + i] is None:
                    x[n - m + i] = y[i]
        x = tuple(x)
        v = d(*x)
    else:
        v = None
    return v

