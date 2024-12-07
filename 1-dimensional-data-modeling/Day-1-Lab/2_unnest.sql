With unnested as (
	SELECT player_name,
	UNNEST(season_stats)::season_stats AS season_stats
	FROM players
)

select * from unnested order by player_name, season_stats asc;

-- This basically lets us explode the season stats, but keeps all the temporal component together
-- So if you needed to explode it and compress it in say parquet, you retain compression
-- performance without having to resort due to run length encoding
