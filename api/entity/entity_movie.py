import asyncio
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class PlatformDetail:
    source: str  # 来源平台名称
    title: str  # 在该平台的电影名称
    cover_url: str  # 封面URL
    create_time: str = field(metadata={"immutable": True})  # 该元数据创建的时间
    directors: List[str] = field(default_factory=list)  # 导演
    actors: List[str] = field(default_factory=list)  # 演员
    release_date: str = ''  # 上映日期
    description: str = ''  # 电影介绍
    score: float = -1.0  # 评分，默认-1.0为暂无评分
    movie_type: List[str] = field(default_factory=list)  # 电影类型
    metadata: Optional[dict] = field(default_factory=dict)  # 其它可存在的元数据


@dataclass
class MovieEntityV2:
    # 主键hash(title)
    _id: str
    # const值 如果存在即不可修改
    create_time: str = field(metadata={"immutable": True})
    # const值 每次都提供值用来更新
    update_time: str = field(metadata={"immutable": True})
    # const值 如果存在即不可修改
    fixed_title: str = field(metadata={"immutable": True})
    # 多平台数据源，在主键相同时会合并新进来的数据
    platform_detail: List[PlatformDetail] = field(default_factory=list)
    # Mapping到MongoDB的表，并在插入对象时排除
    collection: str = field(default='movies', metadata={'exclude': True})
