-- QUESTION 1: What is the seniority distribution in tech job postings?
-- Aggregates tech job postings by year and seniority category.
-- Calculates pct_of_total so you can see relative seniority distribution.
--
-- No partition — this is a small aggregated table (a few hundred rows max).
-- Per Google BigQuery docs, tables under 64MB see negligible benefit from partitioning.
-- Cluster by seniority_category: queries GROUP BY / filter on this in every Looker tile.
-- Materialized as table — marts are always tables, never views.

-- dbt config: partition + cluster defined HERE in the mart, not in raw ingestion.
{{ config(
    materialized='table',
    cluster_by=['seniority_category', 'data_source']
) }}

-- Dataset covers Dec 2023 – Apr 2024 (one year of LinkedIn data).
-- post_year alone is useless for trend analysis with one year.
-- posted_date enables monthly time series in Looker Studio (Tile 1).
-- pct_of_total gives relative distribution for the donut chart (Tile 2).
WITH jobs AS (
  SELECT * FROM {{ ref('int_jobs_unified') }}
  WHERE is_tech_role = TRUE
    AND seniority_category != 'Unknown'
    AND posted_date IS NOT NULL
),

overall_total AS (
  SELECT COUNT(*) AS total_frontend_jobs FROM jobs
),

by_seniority AS (
  SELECT
    posted_date,
    post_year,
    seniority_category,
    data_source,
    COUNT(*) AS job_count
  FROM jobs
  GROUP BY 1, 2, 3, 4
)

SELECT
  s.posted_date,
  s.post_year,
  s.seniority_category,
  s.data_source,
  s.job_count,
  t.total_frontend_jobs,
  ROUND(s.job_count / t.total_frontend_jobs * 100, 4) AS pct_of_total
FROM by_seniority s
CROSS JOIN overall_total t
