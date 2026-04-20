-- QUESTION 1: What is the seniority distribution in frontend job postings?
-- Aggregates frontend job postings by year and seniority category.
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

WITH jobs AS (
  SELECT * FROM {{ ref('int_jobs_unified') }}
  WHERE is_frontend_role = TRUE
    AND seniority_category != 'Unknown'
),

yearly_totals AS (
  SELECT post_year, COUNT(*) AS total_frontend_jobs
  FROM jobs GROUP BY 1
),

by_seniority AS (
  SELECT post_year, seniority_category, data_source, COUNT(*) AS job_count
  FROM jobs GROUP BY 1, 2, 3
)

SELECT
  s.post_year,
  s.seniority_category,
  s.data_source,
  s.job_count,
  t.total_frontend_jobs,
  ROUND(s.job_count / t.total_frontend_jobs * 100, 2) AS pct_of_total
FROM by_seniority s
JOIN yearly_totals t USING (post_year)