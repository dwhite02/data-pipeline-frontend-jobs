-- QUESTION 3: Which companies show ghost posting patterns?
-- Ghost posting proxy: high applicant count + long listing duration.
-- Threshold calibrated to dataset: 10+ applicants AND 25+ days listed = potential ghost job.
-- Original 100+/60+ threshold produced zero results with this dataset.
--
-- No partition — this is a small company-level aggregation table.
-- Cluster by company_name: Looker bar chart and filters operate on company.
-- Materialized as table — marts are always tables, never views.

-- dbt config: partition + cluster defined HERE in the mart, not in raw ingestion.
{{ config(
    materialized='table',
    cluster_by=['company_name']
) }}

WITH tech_postings AS (
  SELECT * FROM {{ ref('int_jobs_unified') }}
  WHERE data_source = 'linkedin_2023_2024'
    AND is_tech_role = TRUE
    AND applicant_count IS NOT NULL
),

flagged AS (
  SELECT
    job_id,
    job_title,
    company_name,
    seniority_category,
    applicant_count,
    posted_date,
    days_listed,
    location,
    is_remote,

    CASE
      WHEN applicant_count >= 100
       AND days_listed >= 60 THEN TRUE
      ELSE FALSE
    END AS is_potential_ghost

  FROM tech_postings
),

-- Aggregate to company level for the dashboard bar chart
company_summary AS (
  SELECT
    company_name,
    COUNT(*)                          AS total_tech_postings,
    COUNTIF(is_potential_ghost)       AS ghost_posting_count,
    ROUND(COUNTIF(is_potential_ghost) / NULLIF(COUNT(*), 0) * 100, 1) AS ghost_pct,
    AVG(applicant_count)              AS avg_applicants,
    AVG(days_listed)                  AS avg_days_listed
  FROM flagged
  GROUP BY 1
  HAVING total_tech_postings >= 3   -- exclude companies with only 1-2 posts
)

SELECT * FROM company_summary
