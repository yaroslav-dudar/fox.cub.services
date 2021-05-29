select team, avg, name as season_name from
(
    select team, season_id, avg(score) from
    (select away_team as team, season_id, avg(away_team_points) as score
    from game where event_id = 2 and season_id IN (4)
    group by away_team, season_id

    union

    select home_team as team, season_id, avg(home_team_points) as score
    from game where event_id = 2 and season_id IN (4)
    group by home_team, season_id) as q1
    group by team, season_id) as q2
inner join season on q2.season_id = season.id;

