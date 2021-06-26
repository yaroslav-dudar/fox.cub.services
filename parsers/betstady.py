"""www.betstudy.com scaraper."""
from dataclasses import field
import logging
from datetime import datetime
from typing import Generator

import scrapy
import scrapy.selector

from domain import FootballMatch, Venue
import repository
import parsers.betstady_datasets as betstady_datasets


class MatchPipeline:
    """Pipeline that finalize item with internal data."""

    async def process_item(
        self, item: FootballMatch, spider: "BetStadySpider"
    ) -> FootballMatch:
        season = await spider.season_repo.insert(item.season_id)
        event = await spider.event_repo.insert(item.event_id)
        item.season_id, item.event_id = season.get("id"), event.get("id")
        return item


class BetStadySpider(scrapy.Spider):
    name = "betstady"

    custom_settings = {
        "DOWNLOAD_DELAY": 3,
        "RETRY_TIMES": 5,
        "CONCURRENT_REQUESTS": 1,
        "LOG_LEVEL": "INFO",
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
        "ITEM_PIPELINES": {
            MatchPipeline: 300,
        },
    }

    dataset: betstady_datasets.BetStadyDataset = betstady_datasets.epl
    proxy = "1.0.213.174:8080"

    def __init__(
        self,
        items_buffer: list,
        pg_client: repository.PgClient,
        dataset: betstady_datasets.BetStadyDataset = None,
    ):
        super().__init__()
        if dataset:
            self.dataset = dataset

        self.event_name = f"{self.dataset.region}-{self.dataset.division}"
        self.items_buffer = items_buffer
        self.season_repo = repository.SeasonPgRepository(pg_client)
        self.event_repo = repository.EventPgRepository(pg_client)

    def start_requests(self) -> Generator[scrapy.Request, None, None]:
        for url in self.dataset:
            yield scrapy.Request(
                url=url, callback=self.parse_table, meta={"proxy": self.proxy}
            )

    def get_season_year(self, url: str) -> str:
        season = url.rsplit("/", 2)[-2]
        return season.split("-")[0]

    def get_teams_points(
        self, team1_ft_score: int, team2_ft_score: int
    ) -> tuple[int, int]:
        if team1_ft_score == team2_ft_score:
            return (1, 1)
        elif team1_ft_score > team2_ft_score:
            return (3, 0)
        else:
            return (0, 3)

    def get_teams_ft_score(self, selector: scrapy.selector.Selector) -> tuple[int, int]:
        scoreline = selector.css("strong::text").extract_first()
        scoreline = scoreline.replace(" ", "").split("-") if scoreline else (None, None)
        return int(scoreline[0]), int(scoreline[1])

    def parse_table(
        self, response: scrapy.http.Response
    ) -> Generator[FootballMatch, None, None]:
        games = response.css("table.schedule-table tr")
        season = self.get_season_year(response.request.url)
        for game in games:
            fields = game.css("td")
            if len(fields) < 4:
                continue

            match = FootballMatch()
            try:
                match.season_id = season
                match.event_id = self.event_name
                match.group = -1
                match.team1_ht_score, match.team2_ht_score = None, None
                match.team1_id, match.team2_id = None, None
                match.venue = Venue.TEAM1.value

                date = fields[0].css("::text").extract_first().replace(".", "/")
                match.date = datetime.strptime(date, "%d/%m/%Y")
                match.team1_name = fields[1].css("a::text").extract_first().strip()
                match.team2_name = fields[3].css("a::text").extract_first().strip()

                match.team1_ft_score, match.team2_ft_score = self.get_teams_ft_score(
                    fields[2]
                )
                match.team1_points, match.team2_points = self.get_teams_points(
                    match.team1_ft_score, match.team2_ft_score
                )

                self.items_buffer.append(match)
            except Exception as err:
                logging.warning(
                    "Couldn't parse game: %s with exception: %s", game, str(err)
                )

            yield match
