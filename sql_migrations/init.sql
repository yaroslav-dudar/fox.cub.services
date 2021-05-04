
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_database WHERE datname = 'fox_cub') THEN
        CREATE DATABASE fox_cub;
    ELSE
        raise notice 'Database alrady exists.';
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'fox_cub_root') THEN
        CREATE USER fox_cub_root WITH ENCRYPTED PASSWORD '1234';
    ELSE
        raise notice 'Root user already exists.';
    END IF;
END
$$;

GRANT ALL PRIVILEGES ON DATABASE fox_cub TO fox_cub_root;

CREATE TABLE IF NOT EXISTS game (
    home_team CHAR(64),
    away_team CHAR(64),

    event_id BIGINT,
    season_id BIGINT,
    date DATE,

    home_team_xg FLOAT NULL,
    away_team_xg FLOAT NULL,

    home_team_score INT,
    away_team_score INT,

    home_team_shots INT NULL,
    away_team_shots INT NULL,

    home_team_corners INT NULL,
    away_team_corners INT NULL

) USING columnar;
