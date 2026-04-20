-- QUESTION 2: Are entry-level postings requiring senior-level skills?
-- "Experience inflation" = entry-level job title + demanding skill requirements.
-- Uses the LinkedIn skills table joined back to postings by job_id.
--
-- Partition by posted_date: date range filters are common in analysis queries.
-- Cluster by company_name, location: Looker bar charts and detail views filter/group on these.
-- seniority_category and is_frontend_role are WHERE-clause constants in this mart,
-- so clustering on them adds no pruning benefit.
-- Materialized as table — marts are always tables, never views.

-- dbt config: partition + cluster defined HERE in the mart, not in raw ingestion.
{{ config(
    materialized='table',
    partition_by={
      'field': 'posted_date',
      'data_type': 'date'
    },
    cluster_by=['company_name', 'location']
) }}

WITH entry_jobs AS (
  SELECT * FROM {{ ref('stg_linkedin_jobs') }}
  WHERE is_frontend_role = TRUE
    AND seniority_category = 'Entry Level'
),

job_skills AS (
  SELECT * FROM {{ ref('stg_job_skills') }}
),

skills_analysis AS (
  SELECT
    j.job_id,
    j.job_title,
    j.company_name,
    j.posted_date,
    j.applicant_count,
    j.location,
    COUNT(s.skill_id)            AS total_skill_count,

    -- Count skills that signal senior/complex work in an entry-level posting
    -- skill_abr values are LinkedIn industry category codes, not specific tech names.
    -- Top codes from actual data: IT (Info Tech), ENG (Engineering),
    -- PRJM (Project Mgmt), MGMT (Management), ANLS (Analysis).
    -- Using these as senior-signal codes: entry-level postings requiring
    -- Engineering + IT + Project Mgmt skills signal above-entry scope.
    COUNTIF(s.skill_id IN ('ENG', 'IT', 'PRJM', 'MGMT', 'ANLS'))
                                 AS senior_skill_count,

    -- Inflation score: what % of required skills are senior-level?
    -- High score = entry job that expects senior knowledge
    ROUND(
      COUNTIF(s.skill_id IN ('ENG', 'IT', 'PRJM', 'MGMT', 'ANLS'))
      / NULLIF(COUNT(s.skill_id), 0) * 100, 2
    )                            AS inflation_score

  FROM entry_jobs j
  LEFT JOIN job_skills s USING (job_id)
  GROUP BY 1,2,3,4,5,6
)

SELECT * FROM skills_analysis