DROP MATERIALIZED VIEW IF EXISTS ${warehouse_schema}.tci_area_results;


CREATE MATERIALIZED VIEW ${warehouse_schema}.tci_area_results AS
    SELECT 
        a.registration,
        rs.indicator,
        case when coalesce(sum(rs.score),0)=0 then 0 else 1 end at_risk
    FROM ${warehouse_schema}.xapi_statements a
    JOIN ${warehouse_schema}.tci_surveys s
    ON a.activity_id=s.survey_activity_id
    JOIN ${warehouse_schema}.xapi_statements r
    ON r.registration=a.registration
    JOIN ${warehouse_schema}.tci_response_scores rs
    ON r.statement_id=rs.statement_id
    WHERE a.verb_id='http://adlnet.gov/expapi/verbs/completed'
    AND rs. indicator in ('mood_area','loneliness_area','emotional_state_area','meaningless_area','self_efficacy_area','hope_area')
    GROUP BY  a.registration, rs.indicator
