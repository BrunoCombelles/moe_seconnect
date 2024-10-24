DROP MATERIALIZED VIEW IF EXISTS ${warehouse_schema}.mysec_intents CASCADE;


CREATE MATERIALIZED VIEW ${warehouse_schema}.mysec_intents AS

SELECT intent_activity_id, intent_title
FROM (
  SELECT 
    a.activity_id as intent_activity_id, 
    e.extension_value intent_title, 
    rank() over (partition by s.activity_id order by s."timestamp" desc) rk
FROM ${warehouse_schema}.xapi_activities a
JOIN ${warehouse_schema}.xapi_statements s
ON s.activity_id=a.activity_id
AND s.verb_id='http://activitystrea.ms/schema/1.0/approve'
JOIN ${warehouse_schema}.xapi_extensions e
ON s.statement_id = e.statement_id
AND extension_key = 'https://api&46;learning&46;moe&46;edu&46;sg/title'
WHERE a.activity_id IN
(
  'https://api.learning.moe.edu.sg/lesson/d65edf4c-1923-42b7-a4f8-c80848a637f3', --Intent 1
  'https://api.learning.moe.edu.sg/lesson/48c99f54-24f0-49e5-891e-6411eb6cfba2', -- Intent 2
  'https://api.learning.moe.edu.sg/lesson/b3bbdaef-7ba4-47fd-820b-5d1efa3d804b',
  'https://api.learning.moe.edu.sg/lesson/ca99d4b7-d724-4026-9e37-97be38be681d',
  'https://api.learning.moe.edu.sg/lesson/30134471-8baf-4ad6-98fe-0dca27b4f1d5',
  'https://api.learning.moe.edu.sg/lesson/f664531a-8181-4bbd-8132-c73367d1cd43',
  'https://api.learning.moe.edu.sg/lesson/549b2f32-dcf8-4206-8a2d-fca9e8238576',
  'https://api.learning.moe.edu.sg/lesson/f02b5909-4bea-49ff-be84-d6ad950628dd',
  'https://api.learning.moe.edu.sg/lesson/68bdc1c9-8d33-4f8a-add6-9da077c2622c',
  'https://api.learning.moe.edu.sg/lesson/48c99f54-24f0-49e5-891e-6411eb6cfba2',
  'https://api.learning.moe.edu.sg/lesson/b3bbdaef-7ba4-47fd-820b-5d1efa3d804b',
  'https://api.learning.moe.edu.sg/lesson/d64cc069-3e14-4927-8337-bebbd070f358',
  'https://api.learning.moe.edu.sg/lesson/c21808ee-a298-4e93-bd94-e75597cca9ae',
  'https://api.learning.moe.edu.sg/lesson/abe77e82-d15a-4dc2-8318-faafcc660a7b',
  'https://api.learning.moe.edu.sg/lesson/100a6892-5b0c-4921-9352-e06a245cc4db',
  'https://api.learning.moe.edu.sg/lesson/d667d6e2-f246-4915-91a3-5395adeb56e1',
  'https://api.learning.moe.edu.sg/lesson/37543392-a9d9-4252-912e-cf63d2704a04',
  'https://api.learning.moe.edu.sg/lesson/9756e738-1a80-4272-879a-c00179e7002f',
  'https://api.learning.moe.edu.sg/lesson/4c4d7775-53b0-4421-b5bd-8dceb46b03f0',
  'https://api.learning.moe.edu.sg/lesson/b05b1b04-7887-428d-a265-c779b1dbee8c',
  'https://api.learning.moe.edu.sg/lesson/ebbe7075-3d13-4e65-9346-dde8ba3746f8'
))
WHERE rk=1;
