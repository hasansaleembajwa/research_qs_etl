{{ config(materialized='table') }}

WITH question_history AS (
    SELECT
        question_id,
        year,
        question_text,
        category,
        answer_set,
        LAG(question_text) OVER (PARTITION BY SPLIT_PART(question_id, '_', 1) ORDER BY year) AS previous_question_text,
        LAG(answer_set) OVER (PARTITION BY SPLIT_PART(question_id, '_', 1) ORDER BY year) AS previous_answer_set
    FROM {{ ref('int_research_questions_unified') }}
)

SELECT
    question_id,
    year,
    question_text,
    category,
    answer_set,
    CASE
        WHEN previous_question_text IS NULL THEN 'New Question'
        WHEN question_text != previous_question_text THEN 'Modified Question'
        WHEN answer_set != previous_answer_set THEN 'Modified Answer Set'
        ELSE 'Unchanged'
    END AS evolution_type
FROM question_history