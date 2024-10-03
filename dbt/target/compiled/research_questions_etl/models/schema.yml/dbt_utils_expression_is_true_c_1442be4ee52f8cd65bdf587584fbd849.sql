

with meet_condition as (
    select * from research_db.etl_schema.consolidated_research_questions where 1=1
)

select
    *
from meet_condition

where not(json_array_length(parse_json(answer_set)) > 0)

