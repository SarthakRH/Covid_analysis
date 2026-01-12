# COVID-19 Global Analytics (OWID) — SQL Portfolio Project

## Why this project?
Most COVID projects are just charts. This one is built like a real analytics deliverable:
- clean the data,
- define KPIs correctly (cumulative vs daily),
- generate scorecards + rankings,
- identify waves (peaks),
- connect vaccinations to outcomes (descriptive, not causal claims).

## Dataset
Source: Our World in Data (OWID) COVID dataset  
Tables used:
- Covid deaths (cases, deaths, population, smoothed metrics)
- Covid vaccination (new vaccinations, total vaccinations, etc.)

## Business Questions
1. What are the global and continent-level totals and rates?
2. Which countries were impacted most (raw totals vs per million)?
3. When did major waves occur (local peaks)?
4. How did vaccination rollout progress over time (running totals)?
5. When did a country cross key vaccination thresholds (50/100 doses per 100)?

## KPI Definitions (what I measured)
- **Total Cases (country):** `MAX(total_cases)`  
- **Total Deaths (country):** `MAX(total_deaths)`  
- **Deaths per million:** `1,000,000 * total_deaths / population`  
- **Reported CFR (%):** `100 * total_deaths / total_cases`  
- **7-day rolling trend:** rolling avg using window functions  
- **Doses per 100:** `100 * rolling_doses / population`  
  > Note: doses != people (multiple doses per person)

## Data Cleaning Decisions
- Deduplicated (location, date) rows using ROW_NUMBER and a completeness score.
- Used `TRY_CONVERT` + `NULLIF` to prevent conversion errors and divide-by-zero.
- Filtered `continent IS NOT NULL` to avoid mixing countries with OWID aggregates.

## Key Findings (replace after running the script)
- Dataset range: ____ to ____
- Countries covered: ____
- Global totals: cases ____ , deaths ____ , CFR ____%
- Top countries by total deaths: ____
- Top countries by deaths per million (pop>=1M): ____
- Focus country (India):
  - Peak cases (7d avg): ____ on ____
  - Peak deaths (7d avg): ____ on ____
  - First >= 100 doses/100: ____

## How to run
1. Import the two CSVs into SQL Server as:
   - [Covid deaths ]
   - [Covid vaccination ]
2. Run `sql/covid_analysis.sql` top to bottom in SSMS.
3. Paste your outputs into the “Key Findings” section.

## Next Up (planned)
- Power BI dashboard using views created in SQL
- AWS pipeline: store raw CSVs in S3, query via Athena, visualize in QuickSight/Power BI
