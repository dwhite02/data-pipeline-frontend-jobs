{{ config(
    materialized='table',
    cluster_by=['company_name']
) }}

WITH linkedin_frontend AS (
  SELECT * FROM {{ ref('int_jobs_unified') }}
  WHERE data_source = 'linkedin_2023_2024'
    AND is_frontend_role = TRUE
    AND applicant_count IS NOT NULL
    AND days_listed IS NOT NULL
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

    -- Threshold calibrated to actual dataset:
    -- 142 frontend jobs have both applicant_count and days_listed populated
    -- 10+ applicants AND 25+ days flags 65/142 (46%) — meaningful signal
    -- Original 100+ applicants / 60+ days threshold produced zero results
    CASE
      WHEN applicant_count >= 10
       AND days_listed >= 25 THEN TRUE
      ELSE FALSE
    END AS is_potential_ghost

  FROM linkedin_frontend
),

company_summary AS (
  SELECT
    company_name,
    COUNT(*)                          AS total_frontend_postings,
    COUNTIF(is_potential_ghost)       AS ghost_posting_count,
    ROUND(COUNTIF(is_potential_ghost) / NULLIF(COUNT(*), 0) * 100, 1) AS ghost_pct,
    ROUND(AVG(applicant_count), 1)    AS avg_applicants,
    ROUND(AVG(days_listed), 1)        AS avg_days_listed
  FROM flagged
  GROUP BY 1
  HAVING total_frontend_postings >= 2
)

SELECT * FROM company_summary