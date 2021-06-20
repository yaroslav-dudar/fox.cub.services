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
        self, event_ids: list[int], season_ids: list[int],
        min_points: float, max_points: float
    ) -> list[asyncpg.Record]:
        """
        Finds teams withing given points per game range.
        """
        async with self.client.conn_pool.acquire() as con:
            return await con.fetch('''
                SELECT team, score_per_game, conceded_per_game, points_per_game
                FROM multi_season_table($3, $4)
                WHERE points_per_game <= $2 AND points_per_game >= $1;
            ''', min_points, max_points, event_ids, season_ids)


    async def get_by_score(
        self, event_ids: list[int], season_ids: list[int],
        min_score: float, max_score: float,
        min_conceded: float, max_conceded: float
    ) -> list[asyncpg.Record]:
        async with self.client.conn_pool.acquire() as con:
            return await con.fetch('''
                SELECT team, score_per_game, conceded_per_game, points_per_game
                FROM multi_season_table($1, $2)
                WHERE (score_per_game <= $4 AND score_per_game >= $3)
                    AND (conceded_per_game <= $6 AND conceded_per_game >= $5);
            ''', event_ids, season_ids, min_score, max_score, min_conceded, max_conceded)


    async def get_by_league_pos(
        self, min_pos: float, max_pos: float,
        event_ids: list[int], season_ids: list[int],
    ) -> list[asyncpg.Record]:
        async with self.client.conn_pool.acquire() as con:
            return await con.fetch('''
                SELECT * FROM
                (SELECT team, points,
                    ROW_NUMBER () OVER (PARTITION BY season_id
                                        ORDER BY points DESC)
                    AS league_position FROM season_table($1, $2)
                ) as q
                WHERE league_position >= $3 AND league_position <= $4;

            ''',event_ids, season_ids, min_pos, max_pos)


    async def get_stats(self,
                        event_ids: Optional[list[int]] = None,
                        season_ids: Optional[list[int]] = None,
                        home_teams: Optional[list[str]] = None,
                        away_teams: Optional[list[str]] = None):
         async with self.client.conn_pool.acquire() as con:
            base_query = '''SELECT avg(away_team_points) AS away_points,
                       avg(home_team_points) AS home_points,
                       count(*) AS total_games,
                       avg(away_team_score) AS away_goals,
                       avg(home_team_score) AS home_goals,
                       avg(home_team_score) + avg(away_team_score) AS goals_per_game,

                       count(id) FILTER (WHERE away_team_score + home_team_score > 2.5) / cast(count(*) AS decimal) AS over2_5,
                       count(id) FILTER (WHERE away_team_score + home_team_score > 3.5) / cast(count(*) AS decimal) AS over3_5,
                       count(id) FILTER (WHERE away_team_score > 0 AND home_team_score > 0) / cast(count(*) AS decimal) AS btts,

                       count(id) FILTER (WHERE away_team_score > home_team_score) / cast(count(*) AS decimal) AS away_win,
                       count(id) FILTER (WHERE away_team_score < home_team_score) / cast(count(*) AS decimal) AS home_win,
                       count(id) FILTER (WHERE away_team_score = home_team_score) / cast(count(*) AS decimal) AS draw
                from game where {0} AND {1} AND {2} AND {3};'''

            args = []
            if event_ids:
                args.append(event_ids)
                event_where = f'event_id = ANY(${len(args)}::int[])'
            else:
                event_where = 'event_id IS NOT NULL'

            if season_ids:
                args.append(season_ids)
                season_where = f'season_id = ANY(${len(args)}::int[])'
            else:
                season_where = 'season_id IS NOT NULL'

            if home_teams:
                args.append(home_teams)
                home_teams_where = f'home_team = ANY(${len(args)}::varchar[])'
            else:
                home_teams_where = 'home_team IS NOT NULL'

            if away_teams:
                args.append(away_teams)
                away_teams_where = f'away_team = ANY(${len(args)}::varchar[])'
            else:
                away_teams_where = 'away_team IS NOT NULL'

            final_query = base_query.format(event_where, season_where, home_teams_where, away_teams_where)
            return await con.fetch(final_query, *args)


    async def create_season_table(self) -> None:
        async with self.client.conn_pool.acquire() as con:
            return await con.execute('''
                CREATE OR REPLACE FUNCTION season_table(events integer[], seasons integer[])
                RETURNS TABLE(team VARCHAR,
                              season_id int,
                              points int,
                              points_per_game float,
                              score_per_game float,
                              conceded_per_game float)
                AS $$
                    SELECT
                        team, season_id, sum(points) AS points,
                        sum(points) / sum(games) AS points_per_game,
                        sum(score) / sum(games) AS score_per_game,
                        sum(conceded) / sum(games) AS conceded_per_game FROM

                    (SELECT away_team AS team, season_id, sum(away_team_points) AS points,
                        sum(away_team_score) AS score, sum(home_team_score) AS conceded,
                        count(*) AS games
                        FROM game WHERE event_id = ANY(events)
                        AND season_id = ANY(seasons)
                        GROUP BY away_team, season_id

                    UNION

                    SELECT home_team AS team, season_id, sum(home_team_points) AS points,
                        sum(home_team_score) AS score, sum(away_team_score) AS conceded,
                        count(*) AS games
                        FROM game WHERE event_id = ANY(events)
                        AND season_id = ANY(seasons)
                        GROUP BY home_team, season_id
                    ) as query
                    GROUP BY team, season_id

                $$ LANGUAGE SQL STABLE;''',)


    async def create_multi_season_table(self) -> None:
        async with self.client.conn_pool.acquire() as con:
            return await con.execute('''
                CREATE OR REPLACE FUNCTION multi_season_table(events integer[], seasons integer[])
                RETURNS TABLE(team VARCHAR,
                              points int,
                              points_per_game float,
                              score_per_game float,
                              conceded_per_game float)
                AS $$
                    SELECT
                        team, sum(points) AS points,
                        sum(points) / sum(games) AS points_per_game,
                        sum(score) / sum(games) AS score_per_game,
                        sum(conceded) / sum(games) AS conceded_per_game FROM

                    (SELECT away_team AS team, sum(away_team_points) AS points,
                        sum(away_team_score) AS score, sum(home_team_score) AS conceded,
                        count(*) AS games
                        FROM game WHERE event_id = ANY(events)
                        AND season_id = ANY(seasons)
                        GROUP BY away_team

                    UNION

                    SELECT home_team AS team, sum(home_team_points) AS points,
                        sum(home_team_score) AS score, sum(away_team_score) AS conceded,
                        count(*) AS games
                        FROM game WHERE event_id = ANY(events)
                        AND season_id = ANY(seasons)
                        GROUP BY home_team
                    ) as query
                    GROUP BY team
                $$ LANGUAGE SQL STABLE;''',)


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
