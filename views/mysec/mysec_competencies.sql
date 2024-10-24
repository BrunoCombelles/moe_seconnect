DROP MATERIALIZED VIEW IF EXISTS ${warehouse_schema}.mysec_competencies;

CREATE MATERIALIZED VIEW ${warehouse_schema}.mysec_competencies AS
SELECT competency_activity_id, intent_activity_id, competency_title,question_count
FROM (
SELECT 
    s_comp.activity_id competency_activity_id, 
    i.intent_activity_id intent_activity_id, 
    e.extension_value competency_title, 
    q.question_count, rank() over (partition by s_comp.activity_id order by s_comp."timestamp" DESC) rk
FROM ${warehouse_schema}.mysec_intents i
JOIN ${warehouse_schema}.xapi_context_activities ca
ON ca.activity_id=i.intent_activity_id
AND context_type='parent'
join ${warehouse_schema}.xapi_statements s_comp
on ca.statement_id=s_comp.statement_id
and s_comp.verb_id IN ('http://activitystrea.ms/schema/1.0/approve')
and s_comp.activity_type not in ('http://adlnet.gov/expapi/activities/cmi.interaction')
join ${warehouse_schema}.xapi_extensions e
on s_comp.statement_id=e.statement_id
and e.extension_key='https://api&46;learning&46;moe&46;edu&46;sg/title'
join (
    select ca.activity_id comp_activity_id, count(distinct (s.activity_id)) question_count
    from ${warehouse_schema}.xapi_context_activities ca
    join ${warehouse_schema}.xapi_statements s
    on ca.statement_id=s.statement_id
    and s.verb_id IN ('http://activitystrea.ms/schema/1.0/approve')
    and s.activity_type='http://adlnet.gov/expapi/activities/cmi.interaction'
    where ca.context_type='parent'
    group by ca.activity_id) q
on s_comp.activity_id=q.comp_activity_id)
WHERE rk=1;
