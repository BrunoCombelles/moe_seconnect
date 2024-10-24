DROP MATERIALIZED VIEW IF EXISTS ${warehouse_schema}.mysec_questions cascade;

CREATE MATERIALIZED VIEW ${warehouse_schema}.mysec_questions AS

SELECT 
  c.competency_activity_id competency_activity_id, 
  q.activity_id question_activity_id, 
  ${warehouse_schema}.fct_sanitize_string( a.activity_name) question_activity_name
FROM ${warehouse_schema}.mysec_competencies c
JOIN ${warehouse_schema}.xapi_context_activities ca
ON ca.activity_id=c.competency_activity_id
AND context_type='parent'
join ${warehouse_schema}.xapi_statements q
on ca.statement_id=q.statement_id
and q.verb_id IN ('http://activitystrea.ms/schema/1.0/approve')
and q.activity_type in ('http://adlnet.gov/expapi/activities/cmi.interaction')
join ${warehouse_schema}.xapi_activities a
on q.activity_id=a.activity_id
and q.activity_type=a.activity_type;
