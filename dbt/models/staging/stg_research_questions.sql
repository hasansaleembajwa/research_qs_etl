{{ config(materialized='table') }}

WITH deduplicated AS (
    SELECT
        question_id,
        year,
        question_text,
        CASE 
            WHEN category LIKE 'New %' THEN REPLACE(category, 'New ', '')
            ELSE category
        END AS category,
        is_active,
        valid_from,
        valid_to,
        answer_set,
        question_status,
        ROW_NUMBER() OVER (PARTITION BY question_id, year ORDER BY valid_from DESC) AS rn
    FROM {{ source('raw', 'consolidated_research_questions') }}
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
FROM deduplicated
WHERE rn = 1