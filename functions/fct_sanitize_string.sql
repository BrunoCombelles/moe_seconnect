CREATE OR REPLACE FUNCTION seconnect_dev_warehouse.fct_sanitize_string(character varying)
 RETURNS character varying LANGUAGE sql
 VOLATILE
AS $_$

SELECT 
replace(
replace(
replace(
    regexp_replace($1, '(<([^>]+)>)','',1,'ip') ,
    '&nbsp;',' '
),
    '<br/>',''),
    chr(10),' ')


$_$
