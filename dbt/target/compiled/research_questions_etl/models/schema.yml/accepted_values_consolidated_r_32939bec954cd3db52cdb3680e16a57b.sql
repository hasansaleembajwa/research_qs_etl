
    
    

with all_values as (

    select
        category as value_field,
        count(*) as n_records

    from research_db.etl_schema.consolidated_research_questions
    group by category

)

select *
from all_values
where value_field not in (
    'Demographics','Research Topic A','Research Topic B','Topic C','Topic D'
)


