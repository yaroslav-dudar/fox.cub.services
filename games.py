from collections import namedtuple
from datetime import datetime
from typing import List
import json
import random

required_fields = ["FTAG", "FTHG", "Season", "Date", "AwayTeam", "HomeTeam"]
game_fields = required_fields + ["HTHG", "HTAG", "Group",
               "HomeGoalsTiming", "AwayGoalsTiming"]
dota2_game_fields = game_fields + ["HomeTeamWin", "Type", "Id", "Event"]
cs_game_fields = game_fields + ["Map", "Event", "Id"]

_BaseGameTuple = namedtuple('GameTuple', game_fields,
                         defaults=(None,) * len(game_fields))
_Dota2GameTuple = namedtuple('GameTuple', dota2_game_fields,
                             defaults=(None,) * len(dota2_game_fields))
_CsGoGameTuple = namedtuple('GameTuple', cs_game_fields,
                             defaults=(None,) * len(cs_game_fields))


class GameFactory:
    """Game object creator."""

    @staticmethod
    def get_game_class(raw_game):
        if GameFactory.is_dota2(raw_game):
            return Dota2Game
        elif GameFactory.is_cs(raw_game):
            return CsGoGame
        elif GameFactory.is_default(raw_game):
            return Game
        else:
            raise NameError(f"IncorrectGameType: {storage_type}")

    @staticmethod
    def is_default(raw_game):
        for f in required_fields:
            if f not in raw_game:
                return False

        return True

    @staticmethod
    def is_dota2(raw_game):
        if "HomeTeamWin" in raw_game and "Type" in raw_game:
            if GameFactory.is_default(raw_game):
                return True

        return False

    @staticmethod
    def is_cs(raw_game):
        if "Map" in raw_game and "Event" in raw_game:
            if GameFactory.is_default(raw_game):
                return True

        return False


class BaseGame:
    date_format = '%d/%m/%Y'

    @classmethod
    def is_float(cls, field, val):
        FLOAT_FIELDS = ["FTAG", "FTHG", "HTHG", "HTAG", "Group"]
        return field in FLOAT_FIELDS and val is not None

    def is_total_under(self, total=2.5):
        return self.FTHG + self.FTAG < total

    def get_team_points(self, team):
        if self.is_draw():
            return 1
        if self.HomeTeam == team:
            return 3 if self.is_home_win() else 0
        if self.AwayTeam == team:
            return 3 if self.is_away_win() else 0

    def get_team_goals(self, team, is_score):
        """ Send batch of games to Fox.Cub

        Args:
            team: team name to seatch
            is_score: if True return team goals for if False goals agains
        """
        if self.HomeTeam == team:
            return self.FTHG if is_score else self.FTAG
        if self.AwayTeam == team:
            return self.FTAG if is_score else self.FTHG

    def date_as_datetime(self):
        return self.to_datetime(self.Date)

    def date_as_timestamp(self):
        return self.to_datetime(self.Date).strftime("%s")

    @classmethod
    def from_file(cls, filepath):
        """ Create list of games using input file. """
        games = []
        with open(filepath, 'r') as f:
            data = json.load(f)
            class_obj = GameFactory.get_game_class(data[0])
            for g in data:
                class_obj.clean(g)
                try:
                    games.append(class_obj(**g))
                except ValueError as e:
                    print(e)

        return games

    @classmethod
    def to_datetime(cls, str_date, date_format=None):
        if not date_format:
            date_format = cls.date_format

        return datetime.strptime(str_date, date_format)

    @classmethod
    def clean(cls, raw_game: dict):
        for k in list(raw_game.keys()):
            if k not in cls._fields:
                del raw_game[k]

    def reshuffle(self):
        """ Alert home team to away and away to home """

        if bool(random.getrandbits(1)):
            FTAG = self.FTAG
            HTAG = self.HTAG
            AwayGoalsTiming = self.AwayGoalsTiming
            AwayTeam = self.AwayTeam

            return True, self._replace(FTAG=self.FTHG, HTAG=self.HTHG,
                                 AwayGoalsTiming=self.HomeGoalsTiming,
                                 AwayTeam=self.HomeTeam,
                                 FTHG=FTAG, HTHG=HTAG,
                                 HomeGoalsTiming=AwayGoalsTiming,
                                 HomeTeam=AwayTeam)
        return False, self

    def is_home_win(self):
        return self.FTHG > self.FTAG

    def is_draw(self):
        return self.FTHG == self.FTAG

    def is_away_win(self):
        return self.FTAG > self.FTHG


def base_new(cls, *args, **kwargs):
    obj = super(cls, cls)

    new_args = [float(val) if cls.is_float(field, val) else val
        for field, val in zip(obj.__thisclass__._fields, args)]

    new_kwargs = {field: float(val) if cls.is_float(field, val) else val
        for field, val in kwargs.items()}

    return obj.__new__(cls, *new_args, **new_kwargs)


class Game(_BaseGameTuple, BaseGame):
    __new__ = base_new


class CsGoGame(BaseGame, _CsGoGameTuple):
    __new__ = base_new


class Dota2Game(BaseGame, _Dota2GameTuple):
    __new__ = base_new

    def is_home_win(self):
        return self.HomeTeamWin

    def is_draw(self):
        return False

    def is_away_win(self):
        return not self.HomeTeamWin

    def reshuffle(self):
        """ Alert home team to away and away to home """

        if bool(random.getrandbits(1)):
            FTAG = self.FTAG
            HTAG = self.HTAG
            AwayGoalsTiming = self.AwayGoalsTiming
            AwayTeam = self.AwayTeam
            HomeTeamWin = not self.HomeTeamWin

            return True, self._replace(FTAG=self.FTHG, HTAG=self.HTHG,
                                 AwayGoalsTiming=self.HomeGoalsTiming,
                                 AwayTeam=self.HomeTeam,
                                 FTHG=FTAG, HTHG=HTAG,
                                 HomeGoalsTiming=AwayGoalsTiming,
                                 HomeTeam=AwayTeam, HomeTeamWin=HomeTeamWin)
        return False, self


class Serie:

    """ Store game series list """

    def __init__(self, games: List[BaseGame]):
        self.games = sorted(games, key=CsGoGame.Id.fget)
        self.team_1 = games[0].HomeTeam
        self.team_2 = games[0].AwayTeam

    def best_of(self):
        """ Calculate amount of wins to win whole serie """
        serie_len = len(self.games)
        if serie_len == 1:
            return 1
        if serie_len == 2:
            # bo3
            # TODO: may be bo2
            return 3
        if serie_len == 3:
            # bo3 or bo5
            team1_p = sum([g.get_team_points(self.team_1) for g in self.games])
            team2_p = sum([g.get_team_points(self.team_2) for g in self.games])

            max_points = max([team1_p, team2_p])
            if max_points == 6:
                return 3
            if max_points == 9:
                return 5

        # Unknown format
        return -1


    def get_winner(self):
        """ Get team/player who won a serie """
        pass

    def get_final_score(self):
        pass

    def wins_in_a_row(self, wins: int):
        points = sum([g.get_team_points(self.team_1) for g in self.games[:wins]])
        if points == 0 or points == 3 * wins:
            return True

        return False

    @staticmethod
    def from_season(season):
        """ Generate list of series from season object """
        groups = season.get_groups()
        return [Serie(season.get_group_games(g)) for g in groups]
