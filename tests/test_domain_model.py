from domain import FootballMatch


def test_btts():
    raw_data = {
        "group": 1,
        "team1_ht_score": 0,
        "team2_ht_score": 0,
        "team1_ft_score": 1,
        "team2_ft_score": 0,
    }

    match = FootballMatch(**raw_data)
    assert match.is_btts() == False

    raw_data["team2_ft_score"] = 1
    match = FootballMatch(**raw_data)
    assert match.is_btts() == True
