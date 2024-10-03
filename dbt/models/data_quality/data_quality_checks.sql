{{ config(materialized='table') }}

WITH question_counts AS (
    SELECT 
        year,
        COUNT(*) as question_count,
        COUNT(DISTINCT category) as category_count
    FROM {{ ref('consolidated_research_questions') }}
    GROUP BY year
),
answer_set_checks AS (
    SELECT
        question_id,
        year,
        CASE WHEN JSON_ARRAY_LENGTH(answer_set) = 0 THEN 1 ELSE 0 END as empty_answer_set
    FROM {{ ref('consolidated_research_questions') }}
)

SELECT
    qc.year,
    qc.question_count,
    qc.category_count,
    SUM(asc.empty_answer_set) as empty_answer_set_count
FROM question_counts qc
JOIN answer_set_checks asc ON qc.year = asc.year
GROUP BY qc.year, qc.question_count, qc.category_count