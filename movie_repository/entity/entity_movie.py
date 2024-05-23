from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class MovieEntity:
    title: str
    cover_url: str
    directors: List[str] = field(default_factory=list)
    actors: List[str] = field(default_factory=list)
    release_date: str = ''
    intro: str = ''
    score: float = 0.0
    movie_type: List[str] = field(default_factory=list)
    metadata: Optional[dict] = field(default_factory=dict)
