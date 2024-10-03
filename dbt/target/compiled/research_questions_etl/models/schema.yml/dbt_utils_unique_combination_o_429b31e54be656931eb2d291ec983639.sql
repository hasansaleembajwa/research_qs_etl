





with validation_errors as (

    select
        question_id, year
    from research_db.etl_schema.consolidated_research_questions
    group by question_id, year
    having count(*) > 1

)

select *
from validation_errors


