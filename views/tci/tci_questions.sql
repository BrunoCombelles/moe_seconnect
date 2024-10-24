DROP MATERIALIZED VIEW IF EXISTS ${warehouse_schema}.tci_questions CASCADE;


CREATE MATERIALIZED VIEW ${warehouse_schema}.tci_questions AS
SELECT
    s.survey_activity_id survey_activity_id,
    q.activity_id question_activity_id,
    ${warehouse_schema}.fct_sanitize_string(q.activity_name) question_activity_name,
    question_number
FROM
    ${warehouse_schema}.tci_surveys s
    join (
        SELECT s.activity_id, a.activity_name, ca.activity_id survey_activity_id, rank() over (     partition by s.activity_id     ORDER BY         "timestamp" desc ) rk
        FROM ${warehouse_schema}.xapi_statements s join ${warehouse_schema}.xapi_activities a on s.activity_id = a.activity_id and s.activity_type = a.activity_type JOIN ${warehouse_schema}.xapi_context_activities ca ON ca.statement_id = s.statement_id AND context_type = 'parent'
        WHERE s.verb_id IN ('http://activitystrea.ms/schema/1.0/approve') AND s.activity_type in (     'http://adlnet.gov/expapi/activities/cmi.interaction' )
    ) q ON s.survey_activity_id = q.survey_activity_id
    AND rk = 1
    left join (
        --Term 1
        SELECT 1 question_number, 1 term, '38215341-83a5-470f-956e-6462ae8dc160' question_uuid
        UNION SELECT 2, 1,'9a649993-6261-4c4c-893c-7999e9cecb96'
        UNION SELECT 3, 1,'61d3dcd7-051e-492c-88b5-9ac3b807ff78'
        UNION SELECT 4, 1,'da158e03-8a64-438e-9981-832fde9fafa7'
        UNION SELECT 5, 1,'0c867ae2-43f9-4dd9-a6a0-c0ea3c5a2f93'
        UNION SELECT 6, 1,'e1a7b5ba-0a92-42de-a4a6-6f38744cea30'
        UNION SELECT 7, 1,'e4697e3f-04bc-4bbe-a89b-f87bc9fefd0f'
        UNION SELECT 8, 1,'ba9c960a-3f14-4129-b505-962fd6227def'
        UNION SELECT 9,1,'50c63c48-11b2-447b-85ae-51158748f9f2'
        UNION SELECT 10, 1,'d51a9227-c1d8-4359-a9a8-3c295f181909'
        UNION SELECT 11, 1,'d2bd4cc0-bcb7-4aab-9b56-d0fce683a32b'
        UNION SELECT 12, 1,'cf5d8f66-2252-4936-a727-58d514cd5a74'
        UNION SELECT 13, 1,'d5785da1-c915-46db-9a02-2f5c13626a1e'
        UNION SELECT 14, 1,'3b0b4be3-d003-44d0-b609-f398997efb13'
        UNION SELECT 15, 1,'cca04c90-658f-4c36-aa84-639f51057858'
        UNION --Term 2
        SELECT 1, 2,'f5321964-568b-437c-985d-a9f67f9a4017'
        UNION SELECT 2, 2,'76329a79-3833-4789-86ff-8d37c113bbb1'
        UNION SELECT 3, 2,'3f8b7199-9fab-4079-a9c7-72044730dadd'
        UNION SELECT 4, 2,'4a3f67dd-0195-48f8-9f32-a7ae58b038ce'
        UNION SELECT 5, 2,'eaed0ba9-0df9-4c4a-81d1-37b1473aa8cb'
        UNION SELECT 6, 2,'7ee29310-c779-4845-9199-51d50a5107ca'
        UNION SELECT 7, 2,'c4f5e3c5-54a1-4a05-bacc-e0eec8acd6a8'
        UNION SELECT 8, 2,'81245e74-0299-4429-87fb-9d309559f30a'
        UNION SELECT 9, 2,'b025c7a0-1cf3-4dd3-aad3-32656b9baaa1'
        UNION SELECT 10, 2,'0d1c5168-5320-4199-a991-7d3e743bca26'
        UNION SELECT 11, 2,'292d50dd-da6d-40d5-a05a-4ccf289df071'
        UNION SELECT 12, 2,'c60f978a-3355-4a14-8bf8-28115aefb0a8'
        UNION SELECT 13, 2,'737e46cb-edff-42ee-8aa5-240555131d03'
        UNION SELECT 14, 2,'1e91e785-c08d-4053-88ac-7415f0868f6c'
        UNION SELECT 15, 2,'c4792ea7-f7cd-43e0-80a2-08ab32f86460'
        UNION -- Term 3 New
        SELECT 1, 3,'fbd01c51-dc5f-4915-850e-6016aba82238'
        UNION SELECT 2, 3,'0d3f555a-7d1d-418e-b427-8d83d4bf2b39'
        UNION SELECT 3, 3,'25f3f1d9-6cd7-4e98-9ce5-4ae7985ef5e8'
        UNION SELECT 4, 3,'67f42d0e-0dc7-43dd-bca4-4dba297409c2'
        UNION SELECT 5, 3,'00221383-f997-44d5-b7db-1ef3e8f89b9e'
        UNION SELECT 6, 3,'f3a9c672-4e5e-46b3-9a16-8751812d5506'
        UNION SELECT 7, 3,'29935952-dcd1-496f-b0ab-748c0fc075fd'
        UNION SELECT 8, 3,'b9dd12ae-9e51-4dfa-8c71-755dd9d3e5bb'
        UNION SELECT 9, 3,'6e9d26d3-4e56-4154-838b-3c4e334993ff'
        UNION SELECT 10, 3,'7412f09c-27ad-4a9c-9a50-dbbf0bbae86d'
        UNION SELECT 11, 3,'51b5d39d-6cb7-4e68-a6d0-55a63e1873a1'
        UNION SELECT 12, 3,'4ec5edfd-55c0-4829-ba3a-3a88dc400c6a'
        UNION SELECT 13, 3,'6e69ca82-f5f6-412a-8d44-6559ac4f054c'
        UNION -- Term 4 New
        SELECT 1, 4,'113d49cf-1c5d-483e-b97d-9dbddb98494a'
        UNION SELECT 2, 4,'17a1c8a8-e218-49c9-8772-110015a5bb4b'
        UNION SELECT 3, 4,'658f2a63-0d11-4ad5-a7ff-4b2bd13a09fa'
        UNION SELECT 4, 4,'3eedc80c-f8e9-4031-b496-e5d6f3b1d90d'
        UNION SELECT 5, 4,'86b57033-5388-4685-af54-1bd707eb1732'
        UNION SELECT 6, 4,'63a28001-e234-40b3-bac2-733d2b51980c'
        UNION SELECT 7, 4,'902a370c-fd12-481d-a485-0e5f02ecdd88'
        UNION SELECT 8, 4,'583edd99-2705-43af-8ea9-33272ce07d2a'
        UNION SELECT 9, 4,'80b4c808-dbda-4f6d-821f-872d74bdae0d'
        UNION SELECT 10, 4,'1102941f-a794-453c-adb0-f22b34225050'
        UNION SELECT 11, 4,'0fdc497c-ca59-4ef8-8a4e-5b0514940409'
        UNION SELECT 12, 4,'5903c719-388b-4790-84fb-a4fd86febc86'
    ) data 
    on data.term=s.term
    and q.activity_id = s.survey_activity_id||'/question/'||data.question_uuid;
