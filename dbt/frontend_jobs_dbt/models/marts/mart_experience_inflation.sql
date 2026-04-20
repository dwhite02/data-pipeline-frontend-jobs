-- QUESTION 2: Are entry-level postings requiring senior-level skills?
-- "Experience inflation" = entry-level job title + demanding skill requirements.
-- Uses the LinkedIn skills table joined back to postings by job_id.
--
-- Partition by posted_date: date range filters are common in analysis queries.
-- Cluster by company_name, location: Looker bar charts and detail views filter/group on these.
-- seniority_category and is_tech_role are WHERE-clause constants in this mart,
-- so clustering on them adds no pruning benefit.
-- Materialized as table — marts are always tables, never views.

-- dbt config: partition + cluster defined HERE in the mart, not in raw ingestion.
{{ config(
    materialized='table',
    cluster_by=['company_name']
) }}

-- QUESTION 2: Entry-level tech job landscape.
-- skill_abr values are industry codes (IT, ENG etc), not specific tech skills,
-- so skill-based experience inflation analysis is not reliable with this dataset.
-- Instead: surface genuinely useful entry-level signals:
--   - Which companies post the most entry-level tech roles?
--   - How competitive are entry-level roles (applicant counts)?
--   - Remote vs onsite split for entry-level?
--   - How long are entry-level roles listed before expiry?
-- These are real insights this dataset can support.
WITH entry_jobs AS (
  SELECT
    job_id,
    company_name,
    location,
    is_remote,
    applicant_count,
    days_listed
  FROM {{ ref('int_jobs_unified') }}
  WHERE is_tech_role = TRUE
    AND seniority_category = 'Entry Level'
    AND posted_date IS NOT NULL
),

company_summary AS (
  SELECT
    company_name,
    COUNT(*)                              AS entry_level_postings,
    ROUND(AVG(applicant_count), 1)        AS avg_applicants,
    ROUND(AVG(days_listed), 1)            AS avg_days_listed,
    COUNTIF(is_remote = TRUE)             AS remote_count,
    COUNTIF(is_remote = FALSE
            OR is_remote IS NULL)         AS onsite_count,
    ROUND(
      COUNTIF(is_remote = TRUE)
      / NULLIF(COUNT(*), 0) * 100, 1
    )                                     AS remote_pct
  FROM entry_jobs
  WHERE applicant_count IS NOT NULL
  GROUP BY 1
  HAVING entry_level_postings >= 2
)

SELECT * FROM company_summary