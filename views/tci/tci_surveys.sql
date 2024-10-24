DROP MATERIALIZED VIEW seconnect_dev_warehouse.tci_surveys CASCADE;

CREATE MATERIALIZED VIEW seconnect_dev_warehouse.tci_surveys AS



SELECT surveys.activity_id as survey_activity_id, titles.survey_title , surveys.term, surveys.active
FROM (
    SELECT 'https://api.learning.moe.edu.sg/activity/79168774-c5e7-41f3-8d39-9c2ed8caf317' activity_id, 1 Term, True active UNION
    SELECT 'https://api.learning.moe.edu.sg/activity/547a3c35-0674-4792-8aeb-c3700d9b5f4c', 2, True UNION
 --   SELECT 'https://api.learning.moe.edu.sg/activity/8f998b09-caab-4d2d-b21a-cb2fca0a5413', 4, False UNION
--    SELECT 'https://api.learning.moe.edu.sg/activity/b660d57b-8348-4823-a74a-81ef2b5d2015', 3, False UNION
    SELECT 'https://api.learning.moe.edu.sg/activity/7c2940e1-878f-40d8-9088-084e816154cd',3, True UNION
    SELECT 'https://api.learning.moe.edu.sg/activity/814879b7-de32-476a-9248-3a11f697df7b',4, True 

) surveys
JOIN 
    (SELECT s.activity_id, e.extension_value survey_title, rank() over (partition by s.activity_id order by s."timestamp" desc) rk
    FROM seconnect_dev_warehouse.xapi_statements s
    JOIN seconnect_dev_warehouse.xapi_extensions e
    ON s.statement_id = e.statement_id
    AND extension_key = 'https://api&46;learning&46;moe&46;edu&46;sg/title'
    WHERE s.verb_id='http://activitystrea.ms/schema/1.0/approve'
    ) titles

ON titles.activity_id = surveys.activity_id
WHERE rk = 1;
