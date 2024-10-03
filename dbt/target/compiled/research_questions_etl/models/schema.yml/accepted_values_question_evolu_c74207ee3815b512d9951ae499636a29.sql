
    
    

with all_values as (

    select
        evolution_type as value_field,
        count(*) as n_records

    from research_db.etl_schema.question_evolution
    group by evolution_type

)

select *
from all_values
where value_field not in (
    'New Question','Modified Question','Modified Answer Set','Unchanged'
)


