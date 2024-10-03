

with meet_condition as (
    select * from research_db.etl_schema.question_evolution where 1=1
)

select
    *
from meet_condition

where not(evolution_type IN ('New Question', 'Modified Question', 'Modified Answer Set', 'Unchanged'))

