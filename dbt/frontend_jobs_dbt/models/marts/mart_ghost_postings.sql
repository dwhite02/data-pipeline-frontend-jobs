{{ config(
    materialized='table',
    cluster_by=['company_name']
) }}

-- QUESTION 3: Which companies show ghost posting patterns?
-- Ghost posting proxy: roles with 50+ applicants that ran the full 30-day listing window.
-- days_listed is uniformly 30 days in this dataset (LinkedIn standard expiry).
-- High applicant count + full listing window = role attracted demand but may never have been filled.
-- ghost_pct = ghost_posting_count / total_tech_postings — meaningful % per company.

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
      WHEN applicant_count >= 50 THEN TRUE
      ELSE FALSE
    END AS is_potential_ghost
  FROM tech_postings
),

company_summary AS (
  SELECT
    company_name,
    COUNT(*)                                        AS total_tech_postings,
    COUNTIF(is_potential_ghost)                     AS ghost_posting_count,
    ROUND(
      COUNTIF(is_potential_ghost)
      / NULLIF(COUNT(*), 0) * 100, 1
    )                                               AS ghost_pct,
    ROUND(AVG(applicant_count), 1)                  AS avg_applicants,
    ROUND(AVG(days_listed), 1)                      AS avg_days_listed
  FROM flagged
  GROUP BY 1
  HAVING total_tech_postings >= 2
)

SELECT * FROM company_summary