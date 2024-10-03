{{ config(materialized='table') }}

WITH latest_questions AS (
    SELECT
        question_id,
        year,
        question_text,
        category,
        is_active,
        valid_from,
        valid_to,
        answer_set,
        question_status,
        ROW_NUMBER() OVER (PARTITION BY SPLIT_PART(question_id, '_', 2) ORDER BY year DESC) AS rn
    FROM {{ ref('stg_research_questions') }}
)

SELECT
    question_id,
    year,
    question_text,
    category,
    is_active,
    valid_from,
    valid_to,
    answer_set,
    question_status
FROM latest_questions
WHERE rn = 1