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


# 将读取的数据转换为dataclass对象
def mask(v, dataclass_type):
    if isinstance(v, dict):
        # 准备接收转换后的字段
        field_values = {}
        for field_name, field_type in dataclass_type.__annotations__.items():
            field_value = v.get(field_name, None)
            if is_dataclass(field_type):
                # 如果字段类型也是数据类，则递归调用 mask
                field_values[field_name] = mask(field_value, field_type)
            else:
                field_values[field_name] = field_value
        return dataclass_type(**field_values)
    return v
