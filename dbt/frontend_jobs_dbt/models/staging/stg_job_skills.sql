-- Cleans the LinkedIn job_skills supplementary table.
-- Links job_id to skills for use in entry-level competition analysis (Q2).
-- Skills are stored as semicolon-separated strings per job_id.

WITH source AS (
  SELECT * FROM {{ source('frontend_jobs', 'raw_job_skills') }}
)

SELECT
  job_id,
  skill_abr                    AS skill_id
FROM source
WHERE job_id IS NOT NULL
  AND skill_abr IS NOT NULL