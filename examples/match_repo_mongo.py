from pprint import pprint

import repository
import domain

stats = repository.MatchMongoRepository.get_seasons_stats(
    [2020], "england-premier-league", domain.Venue.TEAM2
)

pprint(list(stats))

stats = repository.MatchMongoRepository.get_seasons_stats(
    [2020], "england-premier-league", domain.Venue.TEAM1
)

print("=" * 25)
pprint(list(stats))
