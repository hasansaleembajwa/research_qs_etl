

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
    question_status
FROM research_db.etl_schema_intermediate.int_research_questions_unified
WHERE question_status = 'Current'