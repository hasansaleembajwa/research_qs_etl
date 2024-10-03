

SELECT
    question_id,
    year,
    question_text,
    CASE 
        WHEN category LIKE 'New %' THEN REPLACE(category, 'New ', '')
        ELSE category
    END AS category,
    is_active,
    TRY_PARSE_JSON(answer_set) AS answer_set,
    TRY_PARSE_JSON(previous_answer_set) AS previous_answer_set,
    valid_to,
    first_seen_year,
    previous_version_id,
    match_score,
    answer_set_version,
    text_changed
FROM RESEARCH_DB.ETL_SCHEMA.consolidated_research_questions