CREATE MATERIALIZED VIEW IF EXISTS ${warehouse_schema}.mysec_criteria_range AS

CREATE MATERIALIZED VIEW ${warehouse_schema}.mysec_criteria_range AS

with ext as 
(   select organisation_id, store_id,statement_id, extension_key, json_parse(extension_value) j
    from  ${warehouse_schema}.xapi_extensions
    where extension_key IN (
        'https://api&46;learning&46;moe&46;edu&46;sg/extensions/criteria-range/criteria-1',
        'https://api&46;learning&46;moe&46;edu&46;sg/extensions/criteria-range/criteria-2'  
        )
    and CAN_JSON_PARSE(extension_value)
)

SELECT  organisation_id, store_id,statement_id, extension_key, 
j.criteriaId criteriaId, j.criteria::varchar criteria, j.rangeid rangeId, j."range"::varchar "range", 
${warehouse_schema}.fct_sanitize_string(j.description::varchar) description 
FROM ext;
