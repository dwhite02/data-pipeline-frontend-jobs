{{ config(
    materialized='view'
) }}

WITH source AS (
  SELECT * FROM {{ source('frontend_jobs', 'raw_linkedin_job_postings') }}
),

cleaned AS (
  SELECT
    job_id,
    company_id,
    company_name,
    title                         AS job_title,
    formatted_experience_level    AS seniority_level,
    formatted_work_type           AS work_type,
    CAST(CAST(remote_allowed AS INT64) AS BOOL) AS is_remote,
    location,
    CAST(applies AS INT64)        AS applicant_count,
    CAST(views AS INT64)          AS listing_views,
    description                   AS job_description,

    -- original_listed_time is UNIX milliseconds as a float (e.g. 1713397508000.0)
    -- Must CAST to INT64 first before TIMESTAMP_MILLIS conversion
    DATE(TIMESTAMP_MILLIS(CAST(CAST(original_listed_time AS FLOAT64) AS INT64))) AS posted_date,
    DATE(TIMESTAMP_MILLIS(CAST(CAST(expiry AS FLOAT64) AS INT64)))               AS expiry_date,

    'linkedin_2023_2024'          AS data_source,
    EXTRACT(YEAR FROM DATE(TIMESTAMP_MILLIS(CAST(CAST(original_listed_time AS FLOAT64) AS INT64)))) AS post_year,

    -- LIKE ANY silently drops matches in some BigQuery contexts — use OR LIKE instead
    CASE WHEN
      LOWER(title) LIKE '%frontend%'
      OR LOWER(title) LIKE '%front-end%'
      OR LOWER(title) LIKE '%front end%'
      OR LOWER(title) LIKE '%react%'
      OR LOWER(title) LIKE '%vue%'
      OR LOWER(title) LIKE '%angular%'
      OR LOWER(title) LIKE '%ui engineer%'
      OR LOWER(title) LIKE '%ui developer%'
      OR LOWER(title) LIKE '%web developer%'
      OR LOWER(title) LIKE '%javascript developer%'
      OR LOWER(title) LIKE '%next.js%'
      OR LOWER(title) LIKE '%typescript developer%'
    THEN TRUE ELSE FALSE END      AS is_frontend_role,

    -- Exact match on confirmed values from the dataset:
    -- 'Entry level' (36,708), 'Mid-Senior level' (41,489), 'Associate' (9,826)
    -- 'Director' (3,746), 'Internship' (1,449), 'Executive' (1,222)
    -- 29,409 rows have empty string — fall to 'Unknown' and are excluded from marts
    CASE formatted_experience_level
      WHEN 'Entry level'       THEN 'Entry Level'
      WHEN 'Associate'         THEN 'Entry Level'
      WHEN 'Internship'        THEN 'Entry Level'
      WHEN 'Mid-Senior level'  THEN 'Mid Level'
      WHEN 'Director'          THEN 'Senior Level'
      WHEN 'Executive'         THEN 'Senior Level'
      ELSE 'Unknown'
    END                           AS seniority_category

  FROM source
  WHERE title IS NOT NULL
    AND formatted_experience_level IS NOT NULL
)

SELECT * FROM cleaned