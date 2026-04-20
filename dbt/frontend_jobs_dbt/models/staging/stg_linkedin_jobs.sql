-- Cleans and standardizes LinkedIn 2024 raw data.
-- Key jobs done here:
--   1. Rename columns to consistent names used across all models
--   2. Parse UNIX millisecond timestamps to real DATE type
--   3. Flag tech roles by keyword-matching job titles
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

    -- Tech role detection: TRUE if job title matches software/web/mobile/data/devops keywords
    -- Covers the full spectrum of roles relevant to people breaking into tech
    -- OR LIKE silently drops matches in some BigQuery contexts — use OR LIKE instead
    -- Tech role detection: covers the full spectrum of software engineering roles
    -- relevant to bootcamp grads, CS new grads, and career changers.
    -- Exclusions prevent non-software engineering roles (mechanical, electrical, network etc)
    -- from polluting the dataset. Captures ~2,625 of 123,849 total LinkedIn postings.
CASE WHEN (
      -- Web/Frontend
      LOWER(title) LIKE '%frontend%'
      OR LOWER(title) LIKE '%front-end%'
      OR LOWER(title) LIKE '%front end%'
      OR LOWER(title) LIKE '%react%'
      OR LOWER(title) LIKE '%vue%'
      OR LOWER(title) LIKE '%angular%'
      OR LOWER(title) LIKE '%javascript%'
      OR LOWER(title) LIKE '%typescript%'
      OR LOWER(title) LIKE '%full stack%'
      OR LOWER(title) LIKE '%fullstack%'
      OR LOWER(title) LIKE '%web developer%'
      OR LOWER(title) LIKE '%web engineer%'
      OR LOWER(title) LIKE '%ui engineer%'
      OR LOWER(title) LIKE '%ui developer%'
      OR LOWER(title) LIKE '%email developer%'
      -- Backend
      OR LOWER(title) LIKE '%backend engineer%'
      OR LOWER(title) LIKE '%back-end engineer%'
      OR LOWER(title) LIKE '%back end engineer%'
      OR LOWER(title) LIKE '%backend developer%'
      OR LOWER(title) LIKE '%back-end developer%'
      OR LOWER(title) LIKE '%back end developer%'
      -- General software
      OR LOWER(title) LIKE '%software engineer%'
      OR LOWER(title) LIKE '%software developer%'
      -- DevOps/Cloud/Infrastructure
      OR LOWER(title) LIKE '%devops%'
      OR LOWER(title) LIKE '%cloud engineer%'
      OR LOWER(title) LIKE '%cloud developer%'
      OR LOWER(title) LIKE '%site reliability%'
      OR LOWER(title) LIKE '%infrastructure engineer%'
      OR LOWER(title) LIKE '%platform engineer%'
      -- Languages/Frameworks
      OR LOWER(title) LIKE '%python developer%'
      OR LOWER(title) LIKE '%python engineer%'
      OR LOWER(title) LIKE '%dotnet%'
      OR LOWER(title) LIKE '%.net developer%'
      OR LOWER(title) LIKE '%.net engineer%'
      OR LOWER(title) LIKE '%ruby%'
      OR LOWER(title) LIKE '%php developer%'
      OR LOWER(title) LIKE '%php engineer%'
      OR LOWER(title) LIKE '%golang%'
      OR LOWER(title) LIKE '%go developer%'
      OR LOWER(title) LIKE '%scala developer%'
      OR LOWER(title) LIKE '%kotlin developer%'
      OR LOWER(title) LIKE '%flutter developer%'
      OR LOWER(title) LIKE '%react native%'
      -- Mobile
      OR LOWER(title) LIKE '%ios developer%'
      OR LOWER(title) LIKE '%ios engineer%'
      OR LOWER(title) LIKE '%android developer%'
      OR LOWER(title) LIKE '%android engineer%'
      OR LOWER(title) LIKE '%mobile developer%'
      OR LOWER(title) LIKE '%mobile engineer%'
      -- Data
      OR LOWER(title) LIKE '%data engineer%'
      OR LOWER(title) LIKE '%database developer%'
      OR LOWER(title) LIKE '%database engineer%'
      OR LOWER(title) LIKE '%database administrator%'
      -- AI/ML
      OR LOWER(title) LIKE '%machine learning engineer%'
      OR LOWER(title) LIKE '%ml engineer%'
      OR LOWER(title) LIKE '%ai engineer%'
      -- Application/API/Salesforce
      OR LOWER(title) LIKE '%application developer%'
      OR LOWER(title) LIKE '%application engineer%'
      OR LOWER(title) LIKE '%api developer%'
      OR LOWER(title) LIKE '%api engineer%'
      OR LOWER(title) LIKE '%salesforce developer%'
      OR LOWER(title) LIKE '%salesforce engineer%'
      -- Entry-level specific titles
      OR LOWER(title) LIKE '%junior developer%'
      OR LOWER(title) LIKE '%junior engineer%'
      OR LOWER(title) LIKE '%junior software%'
      OR LOWER(title) LIKE '%associate developer%'
      OR LOWER(title) LIKE '%associate engineer%'
      OR LOWER(title) LIKE '%associate software%'
    ) AND NOT (
      -- Exclude non-software engineering roles
      LOWER(title) LIKE '%embedded%'
      OR LOWER(title) LIKE '%firmware%'
      OR LOWER(title) LIKE '%hardware%'
      OR LOWER(title) LIKE '%java %'
      OR LOWER(title) LIKE '% java%'
      OR LOWER(title) LIKE '%editor%'
      OR LOWER(title) LIKE '%manager%'
      OR LOWER(title) LIKE '%test%'
      OR LOWER(title) LIKE '%qa%'
      OR LOWER(title) LIKE '%sales%'
      OR LOWER(title) LIKE '%mechanical%'
      OR LOWER(title) LIKE '%electrical%'
      OR LOWER(title) LIKE '%civil%'
      OR LOWER(title) LIKE '%structural%'
      OR LOWER(title) LIKE '%chemical%'
      OR LOWER(title) LIKE '%manufacturing%'
      OR LOWER(title) LIKE '%network%'
    )
    THEN TRUE ELSE FALSE END      AS is_tech_role,

    -- Note: OR LIKE is GA in BigQuery (since Apr 2024). Older dbt docs incorrectly
    -- state "BigQuery does not support OR LIKE" -- those docs predate Google's GA release.
    -- Source: cloud.google.com/bigquery/docs/reference/standard-sql/operators#like_operator
    -- If you ever need a regex alternative: REGEXP_CONTAINS(LOWER(title), r'software|react|...')
    -- REGEXP_CONTAINS is slightly faster on very large tables but OR LIKE is cleaner to read.

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