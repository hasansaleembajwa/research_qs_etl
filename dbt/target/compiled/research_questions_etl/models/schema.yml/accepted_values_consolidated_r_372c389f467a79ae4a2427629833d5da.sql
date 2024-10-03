
    
    

with all_values as (

    select
        year as value_field,
        count(*) as n_records

    from research_db.etl_schema.consolidated_research_questions
    group by year

)

select *
from all_values
where value_field not in (
    '2021','2022','2023'
)


