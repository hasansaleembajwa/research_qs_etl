
    
    

with all_values as (

    select
        is_active as value_field,
        count(*) as n_records

    from research_db.etl_schema_intermediate.int_research_questions_unified
    group by is_active

)

select *
from all_values
where value_field not in (
    'True','False'
)


