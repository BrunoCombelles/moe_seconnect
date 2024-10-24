DROP MATERIALIZED VIEW IF EXISTS ${warehouse_schema}.tci_response_scores CASCADE;
--REFRESH MATERIALIZED VIEW ${warehouse_schema}.tci_response_scores;


CREATE MATERIALIZED VIEW ${warehouse_schema}.tci_response_scores AS
SELECT a.statement_id, a.registration, indicator, sum(c.score) score
FROM ${warehouse_schema}.xapi_statements a
JOIN ${warehouse_schema}.tci_questions q
ON q.question_activity_id=a.activity_id
JOIN ${warehouse_schema}.xapi_statement_choices sc
ON sc.statement_id=a.statement_id
JOIN ${warehouse_schema}.tci_components c
ON c.question_activity_id=sc.activity_id
AND c.component_id=sc.component_id
WHERE verb_id IN (
    'http://adlnet.gov/expapi/verbs/answered',
    'http: //adlnet.gov/expapi/verbs/answered')
GROUP BY a.statement_id, a.registration, indicator;
