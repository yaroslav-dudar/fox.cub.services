from pprint import pprint

import repository
import domain
stats = repository.MatchRepository.get_seasons_stats(
    [2020], "england-premier-league", domain.Venue.TEAM2)

pprint(list(stats))
