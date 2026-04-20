-- Cleans and standardizes LinkedIn 2024 raw data.
-- Key jobs done here:
--   1. Rename columns to consistent names used across all models
--   2. Parse UNIX millisecond timestamps to real DATE type
--   3. Flag frontend roles by keyword-matching job titles
--   4. Normalize seniority labels (LinkedIn uses varied text like "Entry level", "Mid-Senior level")

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
    CAST(CAST(remote_allowed AS INT64) AS BOOL) AS is_remote,  -- FLOAT64 needs INT64 intermediate cast
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

    -- Frontend role detection: TRUE if job title matches frontend keywords
    -- Adjust this list after previewing actual data titles from Phase 3
    CASE WHEN LOWER(title) LIKE ANY (
      '%frontend%', '%front-end%', '%front end%',
      '%react%', '%vue%', '%angular%',
      '%ui engineer%', '%ui developer%',
      '%web developer%', '%javascript developer%',
      '%next.js%', '%typescript developer%'
    ) THEN TRUE ELSE FALSE END    AS is_frontend_role,

    -- Note: LIKE ANY is GA in BigQuery (since Apr 2024). Older dbt docs incorrectly
    -- state "BigQuery does not support LIKE ANY" -- those docs predate Google's GA release.
    -- Source: cloud.google.com/bigquery/docs/reference/standard-sql/operators#like_operator
    -- If you ever need a regex alternative: REGEXP_CONTAINS(LOWER(title), r'frontend|react|...')
    -- REGEXP_CONTAINS is slightly faster on very large tables but LIKE ANY is cleaner to read.

    -- Normalize seniority labels to 3 clean categories
    -- LinkedIn uses labels like "Entry level", "Mid-Senior level", "Director" etc.
    -- Exact match on confirmed values from the dataset:
    -- 'Entry level' (36,708 rows), 'Mid-Senior level' (41,489), 'Associate' (9,826)
    -- 'Director' (3,746), 'Internship' (1,449), 'Executive' (1,222)
    -- 29,409 rows have empty string — these fall to 'Unknown' and are excluded from marts.
    -- Exact values confirmed from actual data:
    -- 'Entry level' (36,708), 'Mid-Senior level' (41,489), 'Associate' (9,826)
    -- 'Director' (3,746), 'Internship' (1,449), 'Executive' (1,222)
    -- 'Senior level' does NOT exist in this dataset — omitted from CASE
    -- Empty string (29,409 rows) → 'Unknown' → filtered out in marts
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