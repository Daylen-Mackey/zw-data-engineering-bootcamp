select * from player_seasons order by player_name asc;

create type season_stats as (
season INTEGER,
gp INTEGER,
pts REAL, reb REAL, ast REAL
)


Create table players (
player_name TEXT,
height TEXT,
college TEXT,
country TEXT,
draft_year TEXT,
draft_round TEXT,
draft_number TEXT,
season_stats season_stats[],
current_season INTEGER,
PRIMARY KEY(player_name, current_season)

)

INSERT INTO players
WITH yesterday AS (Select * from players where current_season = 1995),
today as (select * from player_seasons where season = 1996)

	SELECT COALESCE(t.player_name, y.player_name) as player_name,
	COALESCE(t.height, y.height) as height,
	COALESCE(t.college, y.college) as college,
	COALESCE(t.draft_year, y.draft_year) as draft_year,
	COALESCE(t.draft_round, y.draft_round) as draft_round,
	COALESCE(t.country, y.country) as country,
	COALESCE(t.draft_number, y.draft_number) as draft_number,

	CASE WHEN y.season_stats IS NULL
		THEN ARRAY[ROW(
		t.season, t.gp,t.pts,t.reb,t.ast)::season_stats]

	WHEN t.season IS NOT NULL THEN  y.season_stats || ARRAY[ROW(
		t.season, t.gp,t.pts,t.reb,t.ast)::season_stats]
	ELSE y.season_stats
	END as season_stats,

	COALESCE(t.season,y.current_season + 1) as current_season


FROM today t FULL OUTER JOIN yesterday y
on t.player_name = y.player_name

-- Select * from today t FULL OUTER JOIN yesterday y
-- ON t.player_name = y.player_name

