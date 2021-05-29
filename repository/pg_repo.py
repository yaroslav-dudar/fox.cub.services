# pylint: disable=E1101
"""Pymongo base wrapper"""

import atexit
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

    async def get_by_event(self, event_id: int):
        async with self.client.conn_pool.acquire() as con:
            return await con.fetch('''
                SELECT * FROM game WHERE event_id = $1
            ''', event_id)


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
