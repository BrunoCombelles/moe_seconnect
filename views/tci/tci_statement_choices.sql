
DROP MATERIALIZED VIEW IF EXISTS  ${warehouse_schema}.tci_statement_choices;

DROP MATERIALIZED VIEW IF EXISTS  seconnect_dev_warehouse.tci_statement_choices;

CREATE MATERIALIZED VIEW seconnect_dev_warehouse.tci_statement_choices AS
SELECT 
    sc.statement_id, 
    sc.component_id,
    CASE WHEN
        -- Term 1 - Q8
        --The option "not worried" is not valid if student picked a "worried" option
        sc.activity_id='https://api.learning.moe.edu.sg/activity/79168774-c5e7-41f3-8d39-9c2ed8caf317/question/ba9c960a-3f14-4129-b505-962fd6227def'
        and sc.component_id='10000000850406'
        and  rs_worries_score.statement_id is not null THEN False
    WHEN
        -- Term 2 - Q4
        --The option "not worried" is not valid if student picked a "worried" option
        sc.activity_id='https://api.learning.moe.edu.sg/activity/547a3c35-0674-4792-8aeb-c3700d9b5f4c/question/4a3f67dd-0195-48f8-9f32-a7ae58b038ce'
        and sc.component_id='10000000849904'
        and  rs_worries_score.statement_id is not null THEN False
    WHEN
        -- Term 2 - Q10
        sc.activity_id='https://api.learning.moe.edu.sg/activity/547a3c35-0674-4792-8aeb-c3700d9b5f4c/question/0d1c5168-5320-4199-a991-7d3e743bca26'
        and sc.component_id<>'10000000849929'
        and  rs_coping_noone_score.statement_id is not null THEN False
    WHEN
        -- Term 3 - Q6
        sc.activity_id='https://api.learning.moe.edu.sg/activity/7c2940e1-878f-40d8-9088-084e816154cd/question/f3a9c672-4e5e-46b3-9a16-8751812d5506'
        and sc.component_id='10000000894793'
        and  rs_stress_score.statement_id is not null THEN False
    WHEN
        -- Term 3 - Q8
        sc.activity_id='https://api.learning.moe.edu.sg/activity/7c2940e1-878f-40d8-9088-084e816154cd/question/b9dd12ae-9e51-4dfa-8c71-755dd9d3e5bb'
        and sc.component_id='10000000894809'
        and rs_challenge_score.statement_id is not null THEN False
    WHEN
        -- Term 4 - Q6
        sc.activity_id='https://api.learning.moe.edu.sg/activity/814879b7-de32-476a-9248-3a11f697df7b/question/63a28001-e234-40b3-bac2-733d2b51980c'
        and sc.component_id='10000000890782'
        and  rs_stress_score.statement_id is not null THEN False
    WHEN
        -- Term 4 - Q7
        sc.activity_id='https://api.learning.moe.edu.sg/activity/814879b7-de32-476a-9248-3a11f697df7b/question/902a370c-fd12-481d-a485-0e5f02ecdd88'
        and sc.component_id='10000000890792' 	
        and  rs_worries_score.statement_id is not null THEN False
    ELSE
        True
    END valid_response
FROM seconnect_dev_warehouse.xapi_statement_choices sc
LEFT JOIN
    (SELECT statement_id
    FROM seconnect_dev_warehouse.tci_response_scores
    WHERE indicator='worries_score'
    AND score>0) rs_worries_score
ON sc.statement_id=rs_worries_score.statement_id
LEFT JOIN
    (
        SELECT statement_id
    FROM seconnect_dev_warehouse.tci_response_scores
    WHERE indicator='coping_noone_score'
    AND score>0) rs_coping_noone_score
ON sc.statement_id=rs_coping_noone_score.statement_id
LEFT JOIN
    (SELECT statement_id
    FROM seconnect_dev_warehouse.tci_response_scores
    WHERE indicator='stress_score'
    AND score>0) rs_stress_score
ON sc.statement_id=rs_stress_score.statement_id
LEFT JOIN
    (SELECT statement_id
    FROM seconnect_dev_warehouse.tci_response_scores
    WHERE indicator='challenge_score'
    AND score>0) rs_challenge_score
ON sc.statement_id=rs_challenge_score.statement_id;
