DROP MATERIALIZED VIEW IF EXISTS ${warehouse_schema}.xapi_persona_attributes_multi 

CREATE MATERIALIZED VIEW ${warehouse_schema}.xapi_persona_attributes_multi
SORTKEY
    (persona_id) AS (
        with n(i) as (
            SELECT
                1
            UNION ALL
            SELECT
                2
            UNION ALL
            SELECT
                3
            UNION ALL
            SELECT
                4
            UNION ALL
            SELECT
                5
            UNION ALL
            SELECT
                6
            UNION ALL
            SELECT
                7
            UNION ALL
            SELECT
                8
            UNION ALL
            SELECT
                9
            UNION ALL
            SELECT
                10
        )
        SELECT
            persona_id,
            split_part(subjectGroups, '|', i) as subjectGroups
        FROM
            ${warehouse_schema}.xapi_persona_attributes
            CROSS JOIN n
    );
