





with validation_errors as (

    select
        question_id, valid_from
    from research_db.etl_schema.consolidated_research_questions
    group by question_id, valid_from
    having count(*) > 1

)

select *
from validation_errors


