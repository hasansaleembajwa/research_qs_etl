{{ config(materialized='table') }}

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
FROM {{ ref('int_research_questions_unified') }}
WHERE question_status = 'Current'