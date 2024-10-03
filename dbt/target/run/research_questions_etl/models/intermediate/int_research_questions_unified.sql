
  
    

        create or replace transient table research_db.etl_schema_intermediate.int_research_questions_unified
         as
        (

WITH questions AS (
    SELECT
        question_id,
        year,
        question_text,
        category,
        is_active,
        answer_set,
        previous_answer_set,
        valid_to,
        first_seen_year,
        previous_version_id,
        match_score,
        answer_set_version,
        text_changed
    FROM research_db.etl_schema_staging.stg_research_questions
),

latest_questions AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY SPLIT_PART(question_id, '_', 2) ORDER BY year DESC) AS rn
    FROM questions
)

SELECT
    question_id,
    year,
    question_text,
    category,
    is_active,
    answer_set,
    previous_answer_set,
    valid_to,
    first_seen_year,
    previous_version_id,
    match_score,
    answer_set_version,
    text_changed,
    CASE 
        WHEN is_active AND valid_to IS NULL THEN 'Current'
        WHEN NOT is_active AND valid_to IS NOT NULL THEN 'Deprecated'
        ELSE 'Historical'
    END AS question_status
FROM latest_questions
WHERE rn = 1
        );
      
  