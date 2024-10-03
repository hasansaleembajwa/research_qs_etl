
    
    

with child as (
    select question_id as from_field
    from research_db.etl_schema.question_evolution
    where question_id is not null
),

parent as (
    select question_id as to_field
    from research_db.etl_schema.consolidated_research_questions
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


