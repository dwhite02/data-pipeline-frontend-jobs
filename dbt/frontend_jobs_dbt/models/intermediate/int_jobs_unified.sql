-- Enriches LinkedIn job postings with skills and industry data.
-- This is the single source of truth for all mart models.
-- Joins postings → skills → industries for full picture per job.

WITH jobs AS (
  SELECT * FROM {{ ref('stg_linkedin_jobs') }}
),

skills AS (
  SELECT
    job_id,
    COUNT(*)          AS skill_count
  FROM {{ ref('stg_job_skills') }}
  GROUP BY 1
)

SELECT
  j.*,
  COALESCE(s.skill_count, 0) AS skill_count,
  -- NULL here means the job_id has no rows in stg_job_skills.
  -- The posting exists but LinkedIn didn't surface skills for it.
  -- COALESCE(x, 0) prevents those jobs from being silently dropped by mart aggregations.
  -- Jobs with skill_count = 0 are included in Q2 but will have inflation_score = NULL.

  -- Listing duration: days between posted and expiry dates.
  -- Use expiry_date - posted_date (NOT CURRENT_DATE) because this is a historical snapshot.
  -- CURRENT_DATE() would make every 2023-2024 posting appear 700+ days old,
  -- making the 60-day ghost posting threshold useless.
  -- Rows with no expiry_date produce NULL here (filtered in mart_ghost_postings).
  DATE_DIFF(j.expiry_date, j.posted_date, DAY) AS days_listed

FROM jobs j
LEFT JOIN skills s USING (job_id)
WHERE j.posted_date IS NOT NULL