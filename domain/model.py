""" Match entity: domain model and data model"""
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum

class MatchType(Enum):
    FOOTBALL = 'football'
    CS_GO    = 'csgo'
    DOTA2    = 'dota2'

class Venue(Enum):
    TEAM1    = 'team1'
    TEAM2    = 'team2'
    NEUTRAL  = 'neutral'


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

    venue: Venue

    team1_id: str
    team2_id: str

    match_type: MatchType
    event_id: int
    season_id: int


@dataclass
class FootballMatch(BaseMatch):
    group: int
    team1_ht_score: int
    team2_ht_score: int

    team1_goals_time: list = None
    team2_goals_time: list = None

    team_1xg: float = field(default=0.0)
    team_2xg: float = field(default=0.0)

    def __init__(self, **kwargs):
        for attr, value in kwargs.items():
            self.__dict__[attr] = value

        self.match_type = MatchType.FOOTBALL.value

    def is_btts(self) -> bool:
        return self.team1_ft_score and self.team2_ft_score

    def is_home_win(self) -> bool:
        return self.team1_ft_score and self.team2_ft_score

    def is_over(self, total: float = 2.5) -> bool:
        return (self.team1_ft_score + self.team2_ft_score) > total

    def as_dict(self) -> dict:
        return asdict(self)
