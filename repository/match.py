"""Pymongo repository for match entity."""
import json
import logging

import pymongo.errors
from bson.objectid import ObjectId

from .base import BaseModel
from domain import FootballMatch, BaseMatch, FootballSeasonStats, Venue

class MatchRepository(metaclass=BaseModel):
    collection = "matches"

    @classmethod
    def search(cls, attr, value) -> FootballMatch:
        if attr == '_id':
            value = ObjectId(value)

        items = cls.db_session.find({ attr: value })
        return [FootballMatch(**i) for i in items]

    @classmethod
    def insert(cls, m: FootballMatch):
        try:
            return cls.db_session.insert_one(m.as_dict())
        except pymongo.errors.DuplicateKeyError:
            logging.warn("Insert is canceled. Duplicated record.")

    @classmethod
    def insert_many(cls, matches: list[FootballMatch]):
        to_insert = [m.as_dict() for m in matches]
        return cls.db_session.insert_many(to_insert, ordered=False)

    @classmethod
    def delete(cls, _id: int):
        return cls.db_session.remove({"_id": ObjectId(_id)})

    @classmethod
    def _get_seasons_stats_group(cls, venue: Venue):
        if venue == Venue.TEAM1:
            team_name = "$team1_name"
            team_score, opponent_score = "$team1_ft_score", "$team2_ft_score"
            team_xg, opponent_xg = "$team_1xg", "$team_2xg"
        elif venue == Venue.TEAM2:
            team_name = "$team2_name"
            team_score, opponent_score = "$team2_ft_score", "$team1_ft_score"
            team_xg, opponent_xg = "$team_2xg", "$team_1xg"
        else:
            raise ValueError(f"{venue} is not supported.")

        return {
            "$group": {
                "_id": {"name": team_name, "season": "$season"},
                "gfpg": { "$avg": team_score },
                "gapg": { "$avg": opponent_score },
                "xgfpg": { "$avg": team_xg },
                "xgapg": { "$avg": opponent_xg },
                "ppg": { "$avg": {
                    "$switch": {
                        "branches": [
                            { "case": { "$gt": [team_score , opponent_score] }, "then": 3 },
                            { "case": { "$eq": [team_score , opponent_score] }, "then": 1 },
                            { "case": { "$lt": [team_score , opponent_score] }, "then": 0 }
                        ],
                        "default": 0
                    }}
                }
            }
        }

    @classmethod
    def get_seasons_stats(
        cls, seasons: list[int], event: str, venue: Venue
    )-> dict[str, FootballSeasonStats]:
        """Performs aggregation to calculate season statistics per team."""
        match = {"$match": {"$and": [{"event": {"$eq": event} }, {"season": {"$in": seasons}}]}}
        group = cls._get_seasons_stats_group(venue)
        return cls.db_session.aggregate([match, group])

    @staticmethod
    def from_file(filepath, domain: BaseMatch):
        """ Create list of matches from input file. """
        matches = []
        with open(filepath, 'r') as _file:
            data = json.load(_file)
            for match in data:
                try:
                    matches.append(domain(**match))
                except ValueError as err:
                    print(err)

        return matches

    @classmethod
    def create_unique_index(cls):
        return cls.db_session.create_index(
            [("date", 1), ("team1_name", 1), ("team2_name", 1)],
            unique=True
        )