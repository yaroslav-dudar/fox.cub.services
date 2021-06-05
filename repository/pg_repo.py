# pylint: disable=E1101
"""Pymongo base wrapper"""

from typing import Optional

import asyncpg
"""PostgreSQL to Python mapper with asyncio support."""

from domain import FootballMatch
from .base import PgClient


class MatchPgRepository:
    def __init__(self, pg_client: PgClient):
        self.client = pg_client

    async def insert_many(self, matches: list[FootballMatch]):
        if not matches:
            return

        input = ((m.team1_name, m.team2_name, m.date,
                  m.event_id, m.season_id, m.team1_ft_score,
                  m.team2_ft_score, m.team1_points, m.team2_points)
                  for m in matches)

        async with self.client.conn_pool.acquire() as con:
            return await con.executemany('''
                INSERT INTO game (
                    home_team, away_team, date, event_id, season_id,
                    home_team_score, away_team_score,
                    home_team_points, away_team_points
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT DO NOTHING
            ''', input)

    async def get_by_event(self, event_id: int) -> list[asyncpg.Record]:
        async with self.client.conn_pool.acquire() as con:
            return await con.fetch('''
                SELECT * FROM game WHERE event_id = $1
            ''', event_id)


    async def get_by_points(
        self, min_points: float, max_points: float,
        event_ids: list[int], season_ids: list[int],
    ) -> list[asyncpg.Record]:
        async with self.client.conn_pool.acquire() as con:
            return await con.fetch('''
                select team, points from
                (
                    select team, avg(points) as points from
                    (select away_team as team, season_id, avg(away_team_points) as points
                    from game where event_id = any($3::int[])
                    and season_id = any($4::int[])
                    group by away_team, season_id

                    union

                    select home_team as team, season_id, avg(home_team_points) as points
                    from game where event_id = any($3::int[])
                    and season_id = any($4::int[])
                    group by home_team, season_id) as q1
                    group by team) as q2
                where points <= $2 and points >= $1;
            ''', min_points, max_points, event_ids, season_ids)

    async def get_stats(self,
                        event_ids: list[int],
                        season_ids: list[int],
                        game_ids: Optional[list[int]] = None):
         async with self.client.conn_pool.acquire() as con:
            return await con.fetch('''
                select avg(away_team_points) as away_points,
                       avg(home_team_points) as home_points,
                       avg(away_team_score) as away_goals,
                       avg(home_team_score) as home_goals,
                       avg(home_team_score) + avg(away_team_score) as goals_per_game,
                       count(id) FILTER (WHERE away_team_score > home_team_score) / cast(count(*) as decimal) as away_win,
                       count(id) FILTER (WHERE away_team_score < home_team_score) / cast(count(*) as decimal) as home_win,
                       count(id) FILTER (WHERE away_team_score = home_team_score) / cast(count(*) as decimal) as draw
                from game where event_id = any($1::int[]) and season_id = any($2::int[]);
            ''', event_ids, season_ids)

class SeasonPgRepository:
    def __init__(self, pg_client: PgClient):
        self.client = pg_client

    async def insert(self, season_name: str):
        async with self.client.conn_pool.acquire() as con:
            return await con.fetchrow('''
                INSERT INTO season (name) VALUES ($1)
                ON CONFLICT(name) DO UPDATE SET name = $1 RETURNING id;
            ''', season_name)


class EventPgRepository:
    def __init__(self, pg_client: PgClient):
        self.client = pg_client

    async def insert(self, ev_name: str):
        async with self.client.conn_pool.acquire() as con:
            return await con.fetchrow('''
                INSERT INTO event (name) VALUES ($1)
                ON CONFLICT(name) DO UPDATE SET name = $1 RETURNING id;
            ''', ev_name)
