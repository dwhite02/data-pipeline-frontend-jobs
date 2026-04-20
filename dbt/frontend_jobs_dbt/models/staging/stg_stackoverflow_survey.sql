-- Extracts AI tool adoption signals from Stack Overflow surveys 2022-2024.
-- Used as context layer for Question 1 (AI correlation with entry-level decline).
-- Columns AISelect, AISent, AIToolCurrently_Using confirmed present across all years.
-- _FILE_NAME pseudo-column removed — does not propagate through dbt source layer.

WITH source AS (
  SELECT * FROM {{ source('frontend_jobs', 'raw_stackoverflow_survey') }}
),

filtered AS (
  SELECT
    ResponseId,
    Country,
    YearsCodePro,
    MainBranch,
    LanguageHaveWorkedWith,
    WebframeHaveWorkedWith,

    -- AI columns confirmed present in all three survey years
    -- AIToolCurrently_Using confirmed as BigQuery column name (spaces → underscores)
    AISelect                  AS ai_use_currently,
    AISent                    AS ai_sentiment,
    AIToolCurrently_Using     AS ai_tools_used

  FROM source
  WHERE MainBranch = 'I am a developer by profession'
)

SELECT * FROM filtered