
    
    

with all_values as (

    select
        question_status as value_field,
        count(*) as n_records

    from research_db.etl_schema.consolidated_research_questions
    group by question_status

)

select *
from all_values
where value_field not in (
    'Current'
)


