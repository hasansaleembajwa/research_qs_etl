
    
    

with all_values as (

    select
        question_status as value_field,
        count(*) as n_records

    from research_db.etl_schema_intermediate.int_research_questions_unified
    group by question_status

)

select *
from all_values
where value_field not in (
    'Current','Historical','Deprecated'
)


