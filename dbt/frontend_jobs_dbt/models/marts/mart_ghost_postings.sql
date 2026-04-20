-- QUESTION 3: Which companies show ghost posting patterns?
-- Ghost posting proxy: high applicant count + long listing duration.
-- Threshold: 100+ applicants AND 60+ days since posted = potential ghost job.
--
-- No partition — this is a small company-level aggregation table.
-- Cluster by company_name: Looker bar chart and filters operate on company.
-- Materialized as table — marts are always tables, never views.

-- dbt config: partition + cluster defined HERE in the mart, not in raw ingestion.
{{ config(
    materialized='table',
    cluster_by=['company_name']
) }}

WITH linkedin_frontend AS (
  SELECT * FROM {{ ref('int_jobs_unified') }}
  WHERE data_source = 'linkedin_2023_2024'
    AND is_frontend_role = TRUE
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

  FROM linkedin_frontend
),

-- Aggregate to company level for the dashboard bar chart
company_summary AS (
  SELECT
    company_name,
    COUNT(*)                          AS total_frontend_postings,
    COUNTIF(is_potential_ghost)       AS ghost_posting_count,
    ROUND(COUNTIF(is_potential_ghost) / NULLIF(COUNT(*), 0) * 100, 1) AS ghost_pct,
    AVG(applicant_count)              AS avg_applicants,
    AVG(days_listed)                  AS avg_days_listed
  FROM flagged
  GROUP BY 1
  HAVING total_frontend_postings >= 3   -- exclude companies with only 1-2 posts
)

SELECT * FROM company_summary