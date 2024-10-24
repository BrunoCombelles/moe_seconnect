DROP MATERIALIZED VIEW IF EXISTS ${warehouse_schema}.xapi_persona_attributes CASCADE;

CREATE MATERIALIZED VIEW ${warehouse_schema}.xapi_persona_attributes
SORTKEY
    (persona_id) AS (
        SELECT
            *
        FROM
            (
                SELECT
                    p.personaid persona_id,
                    coalesce(pa.key,'X') as key,
                    pa.value
                from
                    ${staging_schema}.personas p
                    LEFT JOIN ${staging_schema}.personaattributes pa ON p.personaid = pa.personaid
                    AND p.organisationid = pa.organisationid
            ) PIVOT (
                MAX(value) FOR key IN (
                    'X',
                    'schoolCode',
                    'schoolName',
                    'level',
                    'stream',
                    'formClassCode',
                    'subjectGroups'
                )
            )
    );
