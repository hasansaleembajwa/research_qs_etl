
    
    

select
    question_id as unique_field,
    count(*) as n_records

from research_db.etl_schema.consolidated_research_questions
where question_id is not null
group by question_id
having count(*) > 1


