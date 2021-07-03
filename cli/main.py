"""
Console based module to generate analitical report.
"""

import asyncio
import time
import pprint

from sty import fg, bg, ef, rs

from config import Config
import repository
from helpers.utils import MetricRecord


EPL = "england-premier-league"
J1 = "japan-j1-league"
MLS = "usa-mls"

LAST_5_SEASONS = ["2016", "2017", "2018", "2019", "2020"]
LAST_10_SEASONS = [
    "2011",
    "2012",
    "2013",
    "2014",
    "2015",
    "2016",
    "2017",
    "2018",
    "2019",
    "2020",
]


async def init_client() -> repository.PgClient:
    loop = asyncio.get_running_loop()
    conf = Config()
    pg_client = repository.PgClient(conf["citus_database"], loop)
    await pg_client.init_connection()
    return pg_client


async def get_season_ids(
    season_repo: repository.SeasonPgRepository, season_names: list[str]
) -> list[int]:
    return [r.get("id") for r in await season_repo.get(season_names)]


async def get_event_ids(
    event_repo: repository.EventPgRepository, event_names: list[str]
) -> list[int]:
    return [r.get("id") for r in await event_repo.get(event_names)]


async def fetch_report(
    match_repo: repository.MatchPgRepository,
    event_ids: list[int],
    season_ids: list[int],
    home_team_metrics: dict[str, MetricRecord],
    away_team_metrics: dict[str, MetricRecord],
) -> None:

    await match_repo.create_multi_season_table()
    low_table_teams = await match_repo.get_by_score(
        event_ids, season_ids, **home_team_metrics
    )
    middle_table_teams = await match_repo.get_by_points(
        event_ids, season_ids, **away_team_metrics
    )
    print(fg.li_red)
    pprint.pprint(low_table_teams)
    print(fg.blue)
    pprint.pprint(middle_table_teams)
    print(rs.all, fg.green)

    stats = await match_repo.get_stats(
        event_ids,
        season_ids,
        home_teams=[r["team"] for r in middle_table_teams],
        away_teams=[r["team"] for r in low_table_teams],
    )

    pprint.pprint(dict(stats[0]))
    print(rs.all)



home_team_metrics: dict[str, MetricRecord] = {"score": MetricRecord(0, 1), "conceded": MetricRecord(1, 3)}

away_team_metrics: dict[str, MetricRecord] = {"points": MetricRecord(1.0, 1.8)}

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    pg_client = loop.run_until_complete(init_client())

    match_repo = repository.MatchPgRepository(pg_client)
    season_repo = repository.SeasonPgRepository(pg_client)
    event_repo = repository.EventPgRepository(pg_client)

    selected_events = [MLS]
    season_ids = loop.run_until_complete(get_season_ids(season_repo, LAST_10_SEASONS))
    event_ids = loop.run_until_complete(get_event_ids(event_repo, selected_events))

    start_at = time.time()
    loop.run_until_complete(
        fetch_report(
            match_repo, event_ids, season_ids, home_team_metrics, away_team_metrics
        )
    )
    print(fg.red + f"Execution time: {time.time() - start_at}" + rs.fg)
