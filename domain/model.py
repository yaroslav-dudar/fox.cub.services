""" Match entity: domain model and data model"""
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from typing import Any, Optional, Union


class MatchType(Enum):
    FOOTBALL = "football"
    CS_GO = "csgo"
    DOTA2 = "dota2"


class Venue(Enum):
    TEAM1 = "team1"
    TEAM2 = "team2"
    NEUTRAL = "neutral"


@dataclass
class Season:
    id: int
    name: str


@dataclass
class Event:
    id: int
    name: str


@dataclass
class BaseMatch:
    date: datetime
    team1_name: str
    team2_name: str

    team1_ft_score: int
    team2_ft_score: int

    venue: Union[Venue, str]

    team1_id: Optional[str]
    team2_id: Optional[str]

    match_type: str
    event_id: str
    season_id: str


@dataclass
class FootballMatch(BaseMatch):
    group: int

    team1_points: int
    team2_points: int

    team1_goals_time: Optional[list] = None
    team2_goals_time: Optional[list] = None

    team_1xg: float = field(default=0.0)
    team_2xg: float = field(default=0.0)

    team1_ht_score: Optional[int] = None
    team2_ht_score: Optional[int] = None

    def __init__(self, **kwargs: dict[str, Any]):
        for attr, value in kwargs.items():
            self.__dict__[attr] = value

        self.match_type = MatchType.FOOTBALL.value

    def is_btts(self) -> bool:
        return bool(self.team1_ft_score and self.team2_ft_score)

    def is_home_win(self) -> bool:
        return bool(self.team1_ft_score and self.team2_ft_score)

    def is_over(self, total: float = 2.5) -> bool:
        return (self.team1_ft_score + self.team2_ft_score) > total

    def as_dict(self) -> dict:
        return asdict(self)
