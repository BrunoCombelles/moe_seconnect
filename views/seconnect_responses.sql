DROP MATERIALIZED VIEW IF EXISTS ${warehouse_schema}.seconnect_responses;


CREATE MATERIALIZED VIEW ${warehouse_schema}.seconnect_responses AS

SELECT 
    s.statement_id, 
    ${warehouse_schema}.fct_sanitize_string(coalesce(response_literal, s.response)) response_literal,
    coalesce(rs.score,0) highlight,
        rank() over (partition by s.activity_id, s.persona_id, date_part('year', s."timestamp") order by s."timestamp" desc ) result_order_desc
FROM ${warehouse_schema}.xapi_statements s
LEFT JOIN (
    SELECT sc.statement_id, listagg(component_description ,'; ') within group (order by ic.component_id) as response_literal
    FROM ${warehouse_schema}.xapi_statement_choices sc
    LEFT JOIN ${warehouse_schema}.xapi_interaction_components ic
    ON sc.activity_id=ic.activity_id
    AND sc.component_id=ic.component_id
    GROUP BY sc.statement_id) lit
ON s.statement_id=lit.statement_id
LEFT JOIN ${warehouse_schema}.tci_response_scores rs
ON s.statement_id=rs.statement_id
AND rs.indicator='highlight'
WHERE s.response is not null
;
