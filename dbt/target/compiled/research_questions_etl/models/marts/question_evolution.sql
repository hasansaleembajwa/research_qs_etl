

   WITH question_history AS (
       SELECT
           question_id,
           year,
           question_text,
           category,
           answer_set,
           answer_set_version,
           text_changed,
           previous_answer_set,
           LAG(question_text) OVER (PARTITION BY SPLIT_PART(question_id, '_', 2) ORDER BY year) AS previous_question_text
       FROM research_db.etl_schema_intermediate.int_research_questions_unified
   )

   SELECT
       question_id,
       year,
       question_text,
       category,
       answer_set,
       answer_set_version,
       CASE
           WHEN previous_question_text IS NULL THEN 'New Question'
           WHEN text_changed THEN 'Modified Question'
           WHEN previous_answer_set IS NOT NULL THEN 'Modified Answer Set'
           ELSE 'Unchanged'
       END AS evolution_type,
       previous_question_text,
       previous_answer_set
   FROM question_history
   
-- 

-- WITH question_history AS (
--     SELECT
--         question_id,
--         year,
--         question_text,
--         category,
--         answer_set,
--         LAG(question_text) OVER (PARTITION BY SPLIT_PART(question_id, '_', 1) ORDER BY year) AS previous_question_text,
--         LAG(answer_set) OVER (PARTITION BY SPLIT_PART(question_id, '_', 1) ORDER BY year) AS previous_answer_set
--     FROM research_db.etl_schema_intermediate.int_research_questions_unified
-- )

-- SELECT
--     question_id,
--     year,
--     question_text,
--     category,
--     answer_set,
--     CASE
--         WHEN previous_question_text IS NULL THEN 'New Question'
--         WHEN question_text != previous_question_text THEN 'Modified Question'
--         WHEN answer_set != previous_answer_set THEN 'Modified Answer Set'
--         ELSE 'Unchanged'
--     END AS evolution_type
-- FROM question_history

-- 

-- WITH question_history AS (
--     SELECT
--         CAST(year AS INT) || '_' || question_id AS unique_id,
--         year,
--         question_id,
--         question_text,
--         category,
--         answer_set,
--         LAG(question_text) OVER (PARTITION BY question_id ORDER BY year) AS previous_question_text,
--         LAG(answer_set) OVER (PARTITION BY question_id ORDER BY year) AS previous_answer_set,
--         LAG(year) OVER (PARTITION BY question_id ORDER BY year) AS previous_year,
--         FIRST_VALUE(year) OVER (PARTITION BY question_id ORDER BY year) AS first_year
--     FROM research_db.etl_schema.consolidated_research_questions
-- ),

-- answer_set_changes AS (
--     SELECT
--         unique_id,
--         year,
--         ARRAY_AGG(
--             OBJECT_CONSTRUCT(
--                 'action', 
--                 CASE 
--                     WHEN year = first_year THEN 'initial'
--                     WHEN new_option IS NULL THEN 'removed'
--                     WHEN old_option IS NULL THEN 'added'
--                     WHEN new_option != old_option THEN 'modified'
--                     ELSE 'unchanged'
--                 END,
--                 'old_option', old_option,
--                 'new_option', new_option
--             )
--         ) AS changes
--     FROM (
--         SELECT 
--             qh.unique_id,
--             qh.year,
--             qh.first_year,
--             new.value:OptionText::STRING AS new_option,
--             old.value:OptionText::STRING AS old_option
--         FROM question_history qh
--         LEFT JOIN TABLE(FLATTEN(PARSE_JSON(qh.answer_set))) new
--         FULL OUTER JOIN TABLE(FLATTEN(PARSE_JSON(qh.previous_answer_set))) old
--             ON new.index = old.index
--     )
--     GROUP BY unique_id, year, first_year
-- )

-- SELECT
--     qh.unique_id,
--     qh.year,
--     qh.question_id,
--     qh.question_text,
--     qh.category,
--     qh.answer_set,
--     qh.previous_question_text,
--     qh.previous_answer_set,
--     CASE
--         WHEN qh.year = qh.first_year THEN 'New Question'
--         WHEN qh.question_text != qh.previous_question_text THEN 'Modified Question'
--         WHEN qh.answer_set != qh.previous_answer_set THEN 'Modified Answer Set'
--         ELSE 'Unchanged'
--     END AS evolution_type,
--     asc.changes AS answer_set_changes
-- FROM question_history qh
-- LEFT JOIN answer_set_changes asc ON qh.unique_id = asc.unique_id
-- ORDER BY qh.year, qh.question_id