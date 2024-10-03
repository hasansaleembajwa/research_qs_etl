{{ config(materialized='table') }}

WITH answer_options AS (
    SELECT
        question_id,
        year,
        question_text,
        category,
        f.value:OptionOrder::INT as option_order,
        f.value:OptionText::STRING as option_text
    FROM {{ ref('consolidated_research_questions') }},
    LATERAL FLATTEN(input => PARSE_JSON(answer_set)) f
)

SELECT
    question_id,
    year,
    question_text,
    category,
    option_order,
    option_text,
    COUNT(*) OVER (PARTITION BY question_id) as total_options,
    LAG(option_text) OVER (PARTITION BY question_id, option_order ORDER BY year) as previous_option_text,
    CASE
        WHEN LAG(option_text) OVER (PARTITION BY question_id, option_order ORDER BY year) IS NULL THEN 'New'
        WHEN option_text != LAG(option_text) OVER (PARTITION BY question_id, option_order ORDER BY year) THEN 'Modified'
        ELSE 'Unchanged'
    END as option_change_status
FROM answer_options
ORDER BY year, question_id, option_order