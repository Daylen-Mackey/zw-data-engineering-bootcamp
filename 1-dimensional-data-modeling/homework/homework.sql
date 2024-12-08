-- Question 1: DDL for actors table: Create a DDL for an actors table with the following fields:

-- Define this type first
    -- - `films`: An array of `struct` with the following fields:
	-- 	- film: The name of the film.
	-- 	- votes: The number of votes the film received.
	-- 	- rating: The rating of the film.
	-- 	- filmid: A unique identifier for each film.

CREATE TYPE film_type AS (
    film TEXT,
    votes INTEGER,
    rating REAL,
    filmid TEXT
);

-- quality_class: This field represents an actor's performance quality, determined by the average rating of movies of their most recent year. It's categorized as follows:

-- star: Average rating > 8.
-- good: Average rating > 7 and ≤ 8.
-- average: Average rating > 6 and ≤ 7.
-- bad: Average rating ≤ 6.

CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');


-- is_active: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).

-- Have to use actorID as a primary key, since actor name is not necessarily unique
create table actors (
    primary key (actorid, current_year),
    actor text,
    actorid text,
    films film_type[],
    quality_class quality_class,
    is_active boolean,
    current_year integer
)


-- Cumulative table generation query: Write a query that populates the actors table one year at a time.
-- For our seed query, we need to know the earliest date we start at
-- SELECT MIN(year) from public.actor_films;
-- Starts 1970

INSERT into actors
with yesterday as (select * from actors where current_year = 1969),
today as (
    select actorid,
        actor,
        year,
        array_agg(row(film, votes, rating, filmid)::film_type) as films,
        avg(rating) as avg_rating
    from actor_films
    where year = 1970
    GROUP BY actorid, actor, year
)


select coalesce(t.actor, y.actor) as actor,
coalesce(t.actorid, y.actorid) as actorid,

-- Let's handle films now, we will need to concatenate the films from yesterday with the films from today
case
    when y.films is null then t.films
    when t.films is null then y.films
    else y.films || t.films
end as films,

case
    when t.avg_rating > 8 then 'star'
    when t.avg_rating > 7 then 'good'
    when t.avg_rating > 6 then 'average'
    else 'bad'
end::quality_class as quality_class,



-- Let's handle is_active now
-- Only true if they have films in the current year
case
    when t.films is not null then true
    else false
end as is_active,

coalesce(t.year, y.current_year + 1) as current_year


FROM today t FULL OUTER JOIN yesterday y
on t.actorid = y.actorid


-- QUESTION 3
-- DDL for actors_history_scd table: Create a DDL for an actors_history_scd table with the following features:

-- Implements type 2 dimension modeling (i.e., includes start_date and end_date fields).
-- Tracks quality_class and is_active status for each actor in the actors table.

CREATE TABLE actors_history_scd (
    primary key (actorid, start_date),
    actor text,
    actorid text,
    quality_class quality_class,
    is_active boolean,
    start_date integer,
    end_date integer
)

-- Question 4 Backfill query for actors_history_scd: Write a "backfill"
-- query that can populate the entire actors_history_scd table in a single query.

INSERT INTO actors_history_scd
WITH actor_starting AS (
    SELECT
        actor,
        actorid,
        current_year,
        quality_class,
        is_active,
        -- Detect changes in quality_class and is_active
        LAG(quality_class) OVER (PARTITION BY actorid ORDER BY current_year) <> quality_class
        OR LAG(quality_class) OVER (PARTITION BY actorid ORDER BY current_year) IS NULL AS did_quality_change,
        LAG(is_active) OVER (PARTITION BY actorid ORDER BY current_year) <> is_active
        OR LAG(is_active) OVER (PARTITION BY actorid ORDER BY current_year) IS NULL AS did_activity_change,
        -- Detect breaks in continuity
        LAG(current_year) OVER (PARTITION BY actorid ORDER BY current_year) + 1 <> current_year AS year_gap
    FROM actors
    -- If you wanted to cap the backfill
    -- WHERE current_year <= 1973
),
detect_changes AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        current_year,
        did_quality_change,
        did_activity_change,
        year_gap,
        -- Increment scd_change if there's a change in quality, activity, or year gap
        CASE
            WHEN did_activity_change OR did_quality_change OR year_gap THEN 1
            ELSE 0
        END AS scd_change
    FROM actor_starting
),
actor_change_groups AS (
    SELECT
        actorid,
        actor,
        quality_class,
        is_active,
        current_year,
        SUM(scd_change) OVER (PARTITION BY actorid ORDER BY current_year) AS actor_change_group
    FROM detect_changes
)
SELECT
    actor,
    actorid,
    quality_class,
    is_active,
    MIN(current_year) AS start_year,
    MAX(current_year) AS end_year
FROM actor_change_groups
GROUP BY  actor, actorid, quality_class, is_active, actor_change_group


-- Question 5. **Incremental query for `actors_history_scd`:**
-- Write an "incremental" query that combines the
-- previous year's SCD data with new incoming data
-- from the `actors` table.



-- Create an SCD Type to help
CREATE TYPE scd_type AS (
    quality_class quality_class,
    is_active BOOLEAN,
    start_date INTEGER,
    end_date INTEGER
);
WITH latest_scd_year AS (
    SELECT MAX(end_date) AS latest_scd_year
    FROM actors_history_scd
),
historical_scd as (
    select *
    from actors_history_scd
	where end_date = (select latest_scd_year from latest_scd_year)

),
actual_data as (
    select *
    from actors
    where current_year = (select latest_scd_year + 1 from latest_scd_year)
),
unchanged_records as (
    select
        act.actor,
        act.actorid,
        act.quality_class as quality_class,
        act.is_active,
        hist.start_date,
        act.current_year as end_date
    from actual_data act
    join historical_scd hist
        on act.actorid = hist.actorid
    where act.quality_class = hist.quality_class
        and act.is_active = hist.is_active
),

changed_records as (
    select
        act.actor,
        act.actorid,
        unnest(
            array[
                row(
                    hist.quality_class,
                    hist.is_active,
                    hist.start_date,
                    hist.end_date
                )::scd_type,
                row(
                    act.quality_class,
                    act.is_active,
                    act.current_year,
                    act.current_year
                )::scd_type
            ]
        ) as records

    from actual_data act
    left join historical_scd hist
        on act.actorid = hist.actorid
    where (act.quality_class <> hist.quality_class
        or act.is_active <> hist.is_active)
),
unnested_changed_records as (
    select
        actor,
        actorid,
        (records).quality_class,
        (records).is_active,
        (records).start_date,
        (records).end_date
    from changed_records
),
new_records as (
    select
        act.actor,
        act.actorid,
        act.quality_class as quality_class,
        act.is_active,
        act.current_year as start_date,
        act.current_year as end_date
    from actual_data act
    left join historical_scd hist
        on act.actorid = hist.actorid
    where hist.actorid is null
),
union_records as (

    select *
    from unchanged_records
    union all
    select *
    from unnested_changed_records
    union all
    select *
    from new_records
)

select *
from union_records order by actor ;
