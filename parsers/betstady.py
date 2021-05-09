"""www.betstudy.com scaraper."""
import asyncio
import logging
from datetime  import datetime

import scrapy

from domain import FootballMatch, Venue
import repository

class BetStadyDataset:

    url_pattern = "https://www.betstudy.com/soccer-stats/c/{0}/{1}/d/results/{2}/"

    def __init__(self, region: str, division: str, seasons: list):
        self.current = 0
        self.region = region
        self.division = division
        self.seasons = seasons

    def __iter__(self):
        return self

    def __next__(self):
        if self.current < len(self.seasons):
            url = self.get_url()
            self.current += 1
            return url

        raise StopIteration

    def get_url(self):
        return self.url_pattern.format(self.region,
                                       self.division,
                                       self.seasons[self.current])


class MatchPipeline:
    """Pipeline that finalize item with internal data."""
    async def process_item(self, item: FootballMatch, spider: 'BetStadySpider'):
        season = await spider.season_repo.insert(item.season_id)
        event = await spider.event_repo.insert(item.event_id)
        item.season_id, item.event_id = season.get('id'), event.get('id')
        return item


class BetStadySpider(scrapy.Spider):
    name = 'betstady'

    custom_settings = {
        'DOWNLOAD_DELAY': 3,
        'RETRY_TIMES': 5,
        'CONCURRENT_REQUESTS': 1,
        'LOG_LEVEL': 'INFO',
        'TWISTED_REACTOR': 'twisted.internet.asyncioreactor.AsyncioSelectorReactor',
        'ITEM_PIPELINES': {
            MatchPipeline: 300,
        }
    }

    epl = BetStadyDataset("england", "premier-league",
                          ["{0}-{1}".format(year, year+1)
                          for year in range(2020, 2021)])

    efl_championship = BetStadyDataset("england", "championship",
                                       ["{0}-{1}".format(year, year+1)
                                       for year in range(2005, 2021)])

    belgium_b = BetStadyDataset("belgium", "second-division",
                                ["{0}-{1}".format(year, year+1)
                                for year in range(2005, 2019)])


    a_league = BetStadyDataset("australia", "a-league",
                               ["{0}-{1}".format(year, year+1)
                               for year in range(2005, 2019)])


    poland_1liga = BetStadyDataset("poland", "i-liga",
                                  ["{0}-{1}".format(year, year+1)
                                  for year in range(2005, 2019)])


    ekstraklasa = BetStadyDataset("poland", "ekstraklasa",
                                  ["{0}-{1}".format(year, year+1)
                                  for year in range(2005, 2019)])

    ukr_premier_league = BetStadyDataset("ukraine", "premier-league",
                                         ["{0}-{1}".format(year, year+1)
                                         for year in range(2005, 2020)])

    segunda = BetStadyDataset("spain", "segunda-division",
                              ["{0}-{1}".format(year, year+1)
                              for year in range(2005, 2019)])

    a_bundesliga = BetStadyDataset("austria", "bundesliga",
                                   ["{0}-{1}".format(year, year+1)
                                   for year in range(2005, 2019)])

    au_1liga =BetStadyDataset("austria", "1.-liga",
                                   ["{0}-{1}".format(year, year+1)
                                   for year in range(2005, 2019)])


    den_1div = BetStadyDataset("denmark", "1st-division",
                               ["{0}-{1}".format(year, year+1)
                               for year in range(2005, 2019)])

    eerste_div = BetStadyDataset("netherlands", "eerste-divisie",
                                 ["{0}-{1}".format(year, year+1)
                                 for year in range(2005, 2019)])

    liganos = BetStadyDataset("portugal", "primeira-liga",
                              ["{0}-{1}".format(year, year+1)
                              for year in range(2005, 2019)])

    ger_3liga = BetStadyDataset("germany", "3.-liga",
                              ["{0}-{1}".format(year, year+1)
                              for year in range(2005, 2019)])

    j1_league = BetStadyDataset("japan", "j1-league",
                                ["{0}".format(year)
                                for year in range(2005, 2019)])

    j2_league = BetStadyDataset("japan", "j2-league",
                                ["{0}".format(year)
                                for year in range(2005, 2019)])

    allsvenskan = BetStadyDataset("sweden", "allsvenskan",
                                  ["{0}".format(year)
                                  for year in range(2005, 2019)])

    sc_championship = BetStadyDataset("scotland", "championship",
                                      ["{0}-{1}".format(year, year+1)
                                      for year in range(2005, 2020)])

    sc_premiership = BetStadyDataset("scotland", "premiership",
                                     ["{0}-{1}".format(year, year+1)
                                     for year in range(2005, 2020)])

    croatia_hnl = BetStadyDataset("croatia", "1.-hnl",
                                  ["{0}-{1}".format(year, year+1)
                                  for year in range(2005, 2019)])

    copa_libertadores = BetStadyDataset("south-america", "copa-libertadores",
                                        ["{0}".format(year)
                                        for year in range(2006, 2019)])

    rpl = BetStadyDataset("russia", "premier-league",
                          ["{0}-{1}".format(year, year+1)
                          for year in range(2011, 2020)])

    fnl = BetStadyDataset("russia", "fnl",
                          ["2018-2019","2017-2018","2016-2017",
                          "2010","2009", "2008", "2007", "2006", "2005"])

    czech_liga = BetStadyDataset("czech-republic", "czech-liga",
                          ["{0}-{1}".format(year, year+1)
                          for year in range(2005, 2019)])

    colombia_a = BetStadyDataset("colombia", "primera-a",
                                 ["{0}".format(year)
                                 for year in range(2005, 2019)])

    liga_mx = BetStadyDataset("mexico", "liga-mx",
                              ["{0}-{1}".format(year, year+1)
                              for year in range(2005, 2019)])

    arg_primera = BetStadyDataset("argentina", "primera-division",
                              ["{0}-{1}".format(year, year+1)
                              for year in range(2016, 2020)])

    chile_primera = BetStadyDataset("chile", "primera-division",
                                    ["2019","2018","2016-2017",
                                    "2012","2011"])

    serbia_super_liga = BetStadyDataset("serbia", "super-liga",
                              ["{0}-{1}".format(year, year+1)
                              for year in range(2006, 2019)])

    primera_b_national = BetStadyDataset("argentina", "prim-b-nacional",
                              ["{0}-{1}".format(year, year+1)
                              for year in range(2005, 2019)])

    br_serie_b = BetStadyDataset("brazil", "serie-b",
                              ["{0}".format(year)
                              for year in range(2005, 2019)])

    br_serie_a = BetStadyDataset("brazil", "serie-a",
                              ["{0}".format(year)
                              for year in range(2005, 2019)])

    bahrain_pl = BetStadyDataset("bahrain", "premier-league",
                              ["{0}-{1}".format(year, year+1)
                              for year in range(2005, 2019)])

    bl_pl = BetStadyDataset("belarus", "premier-league",
                             ["{0}".format(year)
                             for year in range(2005, 2019)])

    k_league = BetStadyDataset("korea-republic", "k-league-classic",
                             ["{0}".format(year)
                             for year in range(2005, 2019)])

    k_league2 = BetStadyDataset("korea-republic", "k-league-challenge",
                                ["{0}".format(year)
                                for year in range(2008, 2019)])

    costa_rica_primera = BetStadyDataset("costa-rica", "primera-division",
                                         ["{0}-{1}".format(year, year+1)
                                         for year in range(2005, 2019)])

    nb_1_liga = BetStadyDataset("hungary", "nb-i",
                                ["{0}-{1}".format(year, year+1)
                                for year in range(2005, 2019)])

    fr_ligue1 = BetStadyDataset("france", "ligue-1",
                                ["{0}-{1}".format(year, year+1)
                                for year in range(2005, 2020)])

    dfb_pokal = BetStadyDataset("germany", "dfb-pokal",
                                ["{0}-{1}".format(year, year+1)
                                for year in range(2005, 2020)])

    dataset: BetStadyDataset = epl
    proxy = "144.91.95.126:3128"

    def __init__(self, items_buffer: list, pg_client: repository.PgClient):
        super().__init__()
        self.event_name = f'{self.dataset.region}-{self.dataset.division}'
        self.items_buffer = items_buffer
        self.season_repo = repository.SeasonPgRepository(pg_client)
        self.event_repo = repository.EventPgRepository(pg_client)

    def start_requests(self):
        for url in self.dataset:
            yield scrapy.Request(url=url,
                                 callback=self.parse_table,
                                 meta={'proxy': self.proxy})

    def get_season_year(self, url):
        season = url.rsplit('/', 2)[-2]
        return season.split('-')[0]

    def parse_table(self, response):
        games = response.css('table.schedule-table tr')
        season = self.get_season_year(response.request.url)
        for game in games:
            fields = game.css('td')
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

                date = fields[0].css('::text').extract_first().\
                                        replace(".", "/")
                match.date = datetime.strptime(date, '%d/%m/%Y')
                match.team1_name = fields[1].css('a::text').extract_first().strip()
                match.team2_name = fields[3].css('a::text').extract_first().strip()

                scoreline = fields[2].css('strong::text').extract_first()
                scoreline = (scoreline.replace(' ', '').split("-")
                            if scoreline else (None, None))
                match.team1_ft_score, match.team2_ft_score =\
                    int(scoreline[0]), int(scoreline[1])

                self.items_buffer.append(match)
            except Exception as err:
                logging.warning("Couldn't parse game: %s with exception: %s",
                                game, str(err))

            yield match
