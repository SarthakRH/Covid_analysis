---------------------------------------------------------------
-- 0) PARAMETERS 
---------------------------------------------------------------
IF OBJECT_ID('tempdb..#Params') IS NOT NULL DROP TABLE #Params;
CREATE TABLE #Params (
    FocusCountry  NVARCHAR(100),
    MinPopulation BIGINT
);

INSERT INTO #Params (FocusCountry, MinPopulation)
VALUES ('India', 1000000);


---------------------------------------------------------------
-- 1) DATA QUALITY CHECKS (coverage, duplicates, aggregates)
---------------------------------------------------------------

-- 1A) Dataset date range (raw table)
SELECT
    MIN(CAST([date] AS DATE)) AS dataset_start_date,
    MAX(CAST([date] AS DATE)) AS dataset_end_date
FROM [Covid deaths ];

-- 1B) Basic size
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT location) AS distinct_locations
FROM [Covid deaths ];

-- 1C) Duplicate check (same location + date)
SELECT
    location,
    [date],
    COUNT(*) AS rows_for_same_day
FROM [Covid deaths ]
GROUP BY location, [date]
HAVING COUNT(*) > 1
ORDER BY rows_for_same_day DESC, location, [date];

-- 1D) What “continent IS NULL” contains (aggregates like World, income groups)
SELECT DISTINCT location
FROM [Covid deaths ]
WHERE continent IS NULL
ORDER BY location;


---------------------------------------------------------------
-- 2) CLEAN LAYER (dedupe + safe typing) -> TEMP TABLES
--    Why? Keeps everything in one script and avoids CREATE VIEW permissions.
---------------------------------------------------------------

-- Drop if rerun
IF OBJECT_ID('tempdb..#DeathsClean') IS NOT NULL DROP TABLE #DeathsClean;
IF OBJECT_ID('tempdb..#VaxClean')    IS NOT NULL DROP TABLE #VaxClean;

-- 2A) Clean deaths table
WITH d0 AS (
    SELECT
        d.*,
        ( CASE WHEN total_cases IS NULL THEN 0 ELSE 1 END
        + CASE WHEN new_cases IS NULL THEN 0 ELSE 1 END
        + CASE WHEN total_deaths IS NULL THEN 0 ELSE 1 END
        + CASE WHEN new_deaths IS NULL THEN 0 ELSE 1 END
        + CASE WHEN new_cases_smoothed IS NULL THEN 0 ELSE 1 END
        + CASE WHEN new_deaths_smoothed IS NULL THEN 0 ELSE 1 END
        ) AS completeness_score
    FROM [Covid deaths ] d
),
d1 AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY location, [date]
            ORDER BY completeness_score DESC
        ) AS rn
    FROM d0
)
SELECT
    iso_code,
    continent,
    location,
    CAST([date] AS DATE) AS [date],

    -- Population
    CAST(TRY_CONVERT(FLOAT, population) AS BIGINT) AS population,

    -- Cumulative totals (cast via FLOAT to survive "123.0" strings)
    CAST(TRY_CONVERT(FLOAT, total_cases)  AS BIGINT) AS total_cases,
    CAST(TRY_CONVERT(FLOAT, total_deaths) AS BIGINT) AS total_deaths,

    -- Daily counts (keep as BIGINT for easy sums)
    CAST(TRY_CONVERT(FLOAT, new_cases)  AS BIGINT) AS new_cases,
    CAST(TRY_CONVERT(FLOAT, new_deaths) AS BIGINT) AS new_deaths,

    -- Smoothed metrics (keep FLOAT)
    TRY_CONVERT(FLOAT, new_cases_smoothed)  AS new_cases_smoothed,
    TRY_CONVERT(FLOAT, new_deaths_smoothed) AS new_deaths_smoothed,

    -- Per-million columns (FLOAT)
    TRY_CONVERT(FLOAT, total_cases_per_million)          AS total_cases_per_million,
    TRY_CONVERT(FLOAT, new_cases_per_million)            AS new_cases_per_million,
    TRY_CONVERT(FLOAT, total_deaths_per_million)         AS total_deaths_per_million,
    TRY_CONVERT(FLOAT, new_deaths_per_million)           AS new_deaths_per_million,
    TRY_CONVERT(FLOAT, new_cases_smoothed_per_million)   AS new_cases_smoothed_per_million,
    TRY_CONVERT(FLOAT, new_deaths_smoothed_per_million)  AS new_deaths_smoothed_per_million,

    TRY_CONVERT(FLOAT, reproduction_rate) AS reproduction_rate
INTO #DeathsClean
FROM d1
WHERE rn = 1;

-- 2B) Clean vaccination table
WITH v0 AS (
    SELECT
        v.*,
        ( CASE WHEN new_vaccinations IS NULL THEN 0 ELSE 1 END
        + CASE WHEN total_vaccinations IS NULL THEN 0 ELSE 1 END
        + CASE WHEN people_vaccinated IS NULL THEN 0 ELSE 1 END
        + CASE WHEN people_fully_vaccinated IS NULL THEN 0 ELSE 1 END
        ) AS completeness_score
    FROM [Covid vaccination ] v
),
v1 AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY location, [date]
            ORDER BY completeness_score DESC
        ) AS rn
    FROM v0
)
SELECT
    iso_code,
    continent,
    location,
    CAST([date] AS DATE) AS [date],

    -- New vaccinations can get huge cumulatively => BIGINT
    CAST(TRY_CONVERT(FLOAT, new_vaccinations) AS BIGINT) AS new_vaccinations,
    CAST(TRY_CONVERT(FLOAT, total_vaccinations) AS BIGINT) AS total_vaccinations,
    CAST(TRY_CONVERT(FLOAT, people_vaccinated) AS BIGINT) AS people_vaccinated,
    CAST(TRY_CONVERT(FLOAT, people_fully_vaccinated) AS BIGINT) AS people_fully_vaccinated,
    CAST(TRY_CONVERT(FLOAT, total_boosters) AS BIGINT) AS total_boosters,

    TRY_CONVERT(FLOAT, positive_rate) AS positive_rate
INTO #VaxClean
FROM v1
WHERE rn = 1;


---------------------------------------------------------------
-- 3) COUNTRY SCORECARD (final totals + normalized KPIs)
--    Correct logic: totals are cumulative -> MAX(total_*)
---------------------------------------------------------------

WITH CountryTotals AS (
    SELECT
        continent,
        location,
        MAX(population)    AS population,
        MAX(total_cases)   AS total_cases,
        MAX(total_deaths)  AS total_deaths,
        MAX([date])        AS latest_reporting_date
    FROM #DeathsClean
    WHERE continent IS NOT NULL
    GROUP BY continent, location
)
SELECT
    continent,
    location,
    population,
    total_cases,
    total_deaths,
    latest_reporting_date,
    CAST(100.0 * total_cases  / NULLIF(population, 0) AS DECIMAL(10,2))  AS infected_pct,
    CAST(100.0 * total_deaths / NULLIF(total_cases, 0) AS DECIMAL(10,2)) AS cfr_pct,
    CAST(1000000.0 * total_deaths / NULLIF(population, 0) AS DECIMAL(12,2)) AS deaths_per_million
FROM CountryTotals
ORDER BY total_deaths DESC;


---------------------------------------------------------------
-- 4) GLOBAL TOTALS (sum of country MAX totals — avoids double count)
---------------------------------------------------------------

WITH CountryTotals AS (
    SELECT
        location,
        MAX(total_cases)  AS total_cases,
        MAX(total_deaths) AS total_deaths
    FROM #DeathsClean
    WHERE continent IS NOT NULL
    GROUP BY location
)
SELECT
    SUM(total_cases)  AS global_total_cases,
    SUM(total_deaths) AS global_total_deaths,
    CAST(100.0 * SUM(total_deaths) / NULLIF(SUM(total_cases), 0) AS DECIMAL(10,2)) AS global_cfr_pct
FROM CountryTotals;


---------------------------------------------------------------
-- 5) GLOBAL DAILY TREND + 7-DAY ROLLING (Power BI ready)
---------------------------------------------------------------

WITH GlobalDaily AS (
    SELECT
        [date],
        SUM(COALESCE(new_cases, 0))  AS new_cases,
        SUM(COALESCE(new_deaths, 0)) AS new_deaths
    FROM #DeathsClean
    WHERE continent IS NOT NULL
    GROUP BY [date]
),
GlobalRolling AS (
    SELECT
        [date],
        new_cases,
        new_deaths,
        AVG(CAST(new_cases  AS FLOAT)) OVER (ORDER BY [date] ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS new_cases_7d_avg,
        AVG(CAST(new_deaths AS FLOAT)) OVER (ORDER BY [date] ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS new_deaths_7d_avg
    FROM GlobalDaily
)
SELECT *
FROM GlobalRolling
ORDER BY [date];


---------------------------------------------------------------
-- 6) WAVE PEAK DETECTION (FocusCountry) — no LAG/LEAD
--    Finds local maxima in smoothed cases.
---------------------------------------------------------------

WITH P AS (SELECT FocusCountry FROM #Params),
Focus AS (
    SELECT
        d.location,
        d.[date],
        d.new_cases_smoothed
    FROM #DeathsClean d
    CROSS JOIN P
    WHERE d.continent IS NOT NULL
      AND d.location = P.FocusCountry
      AND d.new_cases_smoothed IS NOT NULL
),
Ordered AS (
    SELECT
        location, [date], new_cases_smoothed,
        ROW_NUMBER() OVER (ORDER BY [date]) AS rn
    FROM Focus
),
Neighbors AS (
    SELECT
        a.location,
        a.[date],
        a.new_cases_smoothed,
        b.new_cases_smoothed AS prev_day,
        c.new_cases_smoothed AS next_day
    FROM Ordered a
    LEFT JOIN Ordered b ON b.rn = a.rn - 1
    LEFT JOIN Ordered c ON c.rn = a.rn + 1
)
SELECT TOP 5
    location,
    [date] AS peak_date,
    new_cases_smoothed AS peak_new_cases_7d_avg
FROM Neighbors
WHERE new_cases_smoothed >= COALESCE(prev_day, -1)
  AND new_cases_smoothed >  COALESCE(next_day, -1)
ORDER BY peak_new_cases_7d_avg DESC;


---------------------------------------------------------------
-- 7) LAG-ADJUSTED CFR (FocusCountry, 14-day lag) — no LAG()
--    deaths_today(7d avg) / cases_14_days_ago(7d avg)
---------------------------------------------------------------

WITH P AS (SELECT FocusCountry FROM #Params),
Focus AS (
    SELECT
        d.[date],
        d.new_cases_smoothed,
        d.new_deaths_smoothed
    FROM #DeathsClean d
    CROSS JOIN P
    WHERE d.continent IS NOT NULL
      AND d.location = P.FocusCountry
      AND d.new_cases_smoothed IS NOT NULL
      AND d.new_deaths_smoothed IS NOT NULL
),
Ordered AS (
    SELECT
        [date],
        new_cases_smoothed,
        new_deaths_smoothed,
        ROW_NUMBER() OVER (ORDER BY [date]) AS rn
    FROM Focus
),
LagJoin AS (
    SELECT
        a.[date],
        a.new_deaths_smoothed,
        b.new_cases_smoothed AS cases_14_days_ago
    FROM Ordered a
    LEFT JOIN Ordered b ON b.rn = a.rn - 14
)
SELECT TOP 30
    [date],
    new_deaths_smoothed,
    cases_14_days_ago,
    CAST(100.0 * new_deaths_smoothed / NULLIF(cases_14_days_ago, 0) AS DECIMAL(10,2)) AS lag_cfr_14_pct
FROM LagJoin
WHERE cases_14_days_ago >= 10000
ORDER BY lag_cfr_14_pct DESC;


---------------------------------------------------------------
-- 8) CONTINENT TOTALS (correct: sum of country totals)
---------------------------------------------------------------

WITH CountryTotals AS (
    SELECT
        continent,
        location,
        MAX(population)    AS population,
        MAX(total_cases)   AS total_cases,
        MAX(total_deaths)  AS total_deaths
    FROM #DeathsClean
    WHERE continent IS NOT NULL
    GROUP BY continent, location
),
ContinentTotals AS (
    SELECT
        continent,
        SUM(population)   AS population,
        SUM(total_cases)  AS total_cases,
        SUM(total_deaths) AS total_deaths
    FROM CountryTotals
    GROUP BY continent
)
SELECT
    continent,
    total_cases,
    total_deaths,
    CAST(1000000.0 * total_deaths / NULLIF(population, 0) AS DECIMAL(12,2)) AS deaths_per_million,
    CAST(100.0 * total_deaths / NULLIF(total_cases, 0) AS DECIMAL(10,2)) AS cfr_pct
FROM ContinentTotals
ORDER BY total_deaths DESC;


---------------------------------------------------------------
-- 9) VACCINATION ROLLING DOSES (FocusCountry)
--    Important: new_vaccinations = DOSES (not unique people)
---------------------------------------------------------------

WITH P AS (SELECT FocusCountry FROM #Params),
Joined AS (
    SELECT
        d.continent,
        d.location,
        d.[date],
        d.population,
        v.new_vaccinations
    FROM #DeathsClean d
    JOIN #VaxClean v
      ON d.location = v.location
     AND d.[date]   = v.[date]
    CROSS JOIN P
    WHERE d.continent IS NOT NULL
      AND d.location = P.FocusCountry
),
Rolling AS (
    SELECT
        continent,
        location,
        [date],
        population,
        new_vaccinations,
        SUM(COALESCE(new_vaccinations, 0)) OVER (PARTITION BY location ORDER BY [date]) AS rolling_doses
    FROM Joined
)
SELECT
    continent,
    location,
    [date],
    population,
    new_vaccinations,
    rolling_doses,
    CAST(100.0 * rolling_doses / NULLIF(population, 0) AS DECIMAL(10,2)) AS doses_per_100
FROM Rolling
ORDER BY [date];


---------------------------------------------------------------
-- 10) VACCINATION THRESHOLD DATES (FocusCountry)
--     When did the country cross 50 and 100 doses per 100?
---------------------------------------------------------------

WITH P AS (SELECT FocusCountry FROM #Params),
Joined AS (
    SELECT
        d.location,
        d.[date],
        d.population,
        v.new_vaccinations
    FROM #DeathsClean d
    JOIN #VaxClean v
      ON d.location = v.location
     AND d.[date]   = v.[date]
    CROSS JOIN P
    WHERE d.continent IS NOT NULL
      AND d.location = P.FocusCountry
),
Rolling AS (
    SELECT
        location,
        [date],
        population,
        SUM(COALESCE(new_vaccinations, 0)) OVER (PARTITION BY location ORDER BY [date]) AS rolling_doses
    FROM Joined
),
Rates AS (
    SELECT
        location,
        [date],
        CAST(100.0 * rolling_doses / NULLIF(population, 0) AS DECIMAL(10,2)) AS doses_per_100
    FROM Rolling
)
SELECT
    MIN(CASE WHEN doses_per_100 >= 50  THEN [date] END) AS first_date_50_doses_per_100,
    MIN(CASE WHEN doses_per_100 >= 100 THEN [date] END) AS first_date_100_doses_per_100
FROM Rates;


---------------------------------------------------------------
-- 11) TOP COUNTRIES BY DEATHS PER MILLION (pop filter from #Params)
---------------------------------------------------------------

WITH P AS (SELECT MinPopulation FROM #Params),
CountryTotals AS (
    SELECT
        d.location,
        MAX(d.population)   AS population,
        MAX(d.total_deaths) AS total_deaths
    FROM #DeathsClean d
    WHERE d.continent IS NOT NULL
    GROUP BY d.location
)
SELECT TOP 15
    c.location,
    c.population,
    c.total_deaths,
    CAST(1000000.0 * c.total_deaths / NULLIF(c.population, 0) AS DECIMAL(12,2)) AS deaths_per_million
FROM CountryTotals c
CROSS JOIN P
WHERE c.population >= P.MinPopulation
ORDER BY deaths_per_million DESC;


---------------------------------------------------------------
-- 12) VALIDATION: “World” row vs sum of country totals
---------------------------------------------------------------

WITH SumCountries AS (
    SELECT
        SUM(mx_cases)  AS sum_country_cases,
        SUM(mx_deaths) AS sum_country_deaths
    FROM (
        SELECT
            location,
            MAX(total_cases)  AS mx_cases,
            MAX(total_deaths) AS mx_deaths
        FROM #DeathsClean
        WHERE continent IS NOT NULL
        GROUP BY location
    ) x
),
WorldRow AS (
    SELECT
        MAX(total_cases)  AS world_cases,
        MAX(total_deaths) AS world_deaths
    FROM #DeathsClean
    WHERE location = 'World'
)
SELECT
    s.sum_country_cases,
    w.world_cases,
    (s.sum_country_cases - w.world_cases) AS cases_diff,
    s.sum_country_deaths,
    w.world_deaths,
    (s.sum_country_deaths - w.world_deaths) AS deaths_diff
FROM SumCountries s
CROSS JOIN WorldRow w;


---------------------------------------------------------------
/* ============================================================
   ✅ KEY FINDINGS TEMPLATE (fill after you run outputs)
   ------------------------------------------------------------
   1) Coverage:
      - Date range: 2020-01-01 to 2024-08-14
      - Distinct Locations: 255 (countries = continent NOT NULL)

   2) Global snapshot:
      - Global cases: 775900191
      - Global deaths: 7058885
      - Global CFR: 0.91%

   3) Rankings:
      - Highest total deaths: Peru	By death count of 22097
  

   4) Focus country (from #Params):
      - Peak wave date (7d avg cases): 2021-12-06 with 681.71
      - Worst lag-adjusted CFR (14d):2021-12-06 with 6.53
      - First >= 50 doses/100: 2021-09-18
      - First >= 100 doses/100: 2022-01-06

   5) Data caveats (important for interviews):
      - reported cases/deaths depend on testing/reporting
      - vaccines metric is doses, not people (multi-dose)
      - OWID has aggregates where continent IS NULL
   ============================================================ */