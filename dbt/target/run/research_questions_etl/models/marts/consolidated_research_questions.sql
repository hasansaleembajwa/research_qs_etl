
  
    

        create or replace transient table research_db.etl_schema.consolidated_research_questions
         as
        (

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
FROM research_db.etl_schema_intermediate.int_research_questions_unified
WHERE question_status = 'Current'
        );
      
  