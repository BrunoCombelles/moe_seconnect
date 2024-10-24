DROP MATERIALIZED VIEW IF EXISTS ${warehouse_schema}.mysec_responses cascade;

CREATE MATERIALIZED VIEW ${warehouse_schema}.mysec_responses AS

SELECT 
    term_code, 
    s.statement_id, 
    s.registration, 
    rank() over (partition by term_code, s.activity_id, s.persona_id order by s."timestamp" desc) latest_in_term,
    rank() over (partition by s.activity_id, s.persona_id order by s."timestamp" desc) latest_all_times,
    lead(s."timestamp") over (partition by s.activity_id, s.persona_id order by s."timestamp" desc) previous_response_date
FROM ${warehouse_schema}.seconnect_terms t
JOIN ${warehouse_schema}.xapi_statements s
ON t.term_start_date<=s."timestamp"
AND t.term_end_date>=s."timestamp"
JOIN ${warehouse_schema}.mysec_competencies c
ON s.activity_id=c.competency_activity_id
WHERE verb_id='http://adlnet.gov/expapi/verbs/completed'
