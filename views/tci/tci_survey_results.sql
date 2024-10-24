DROP MATERIALIZED VIEW IF EXISTS ${warehouse_schema}.tci_survey_results CASCADE;

CREATE MATERIALIZED VIEW ${warehouse_schema}.tci_survey_results AS

SELECT 
    statement_id,
    registration,
    case 
        when t1_i1 > 0 then 1 
        when t2_i1_1 * t2_i1_2 > 0 then 1 
        when t3_i1 > 2 then 1 
        when t4_i1 > 0 then 1 
        else 0 end indicator_1,
    case 
        when t1_i2 > 0 then 1 
        when t2_i2 > 0 then 1 
        when t3_i2_1 * t3_i2_2 > 0 then 1 
        when t4_i2 > 2 then 1 
        else 0 end indicator_2,
    case 
        when t1_i3 > 0 then 1 
        when t2_i3 > 2 then 1 
        when t3_i3 > 0 then 1 
        when t4_i3 > 0 then 1 
        else 0 end indicator_3,
    case 
        when t1_i4 > 2 then 1 
        when t2_i4 > 0 then 1 
        when t3_i4 > 0 then 1 
        when t4_i4 > 0 then 1 
        else 0 end indicator_4,
    case 
        when t1_i5 > 0 then 1 
        when t2_i5 > 0 then 1 
        when t3_i5 > 0 then 1 
        when t4_i5_1 * t4_i5_2 > 0 then 1 
        else 0 end indicator_5,


    case when meaningless_area > 0 then 1 else 0 end meaningless_area,
    case when self_efficacy_area > 0 then 1 else 0 end self_efficacy_area,
    case when hope_area > 0 then 1 else 0 end hope_area,
    case when mood_area > 2 then 1 else 0 end mood_area,
    case when emotional_state_area > 0 then 1 else 0 end emotional_state_area,
    case 
        when stress_others_ind > 0 and stress_self_ind > 0 then 1 
        when stress_self_ind>0 then 2 
        when stress_others_ind>0 then 3  
        else 0 end stress_ind,
    result_order_desc
FROM 
    (SELECT 
        a.statement_id,
        a.registration,
        sum(case when rs.indicator='t1_i1' then coalesce(rs.score,0) else 0 end) t1_i1,
        sum(case when rs.indicator='t2_i1_1' then coalesce(rs.score,0) else 0 end) t2_i1_1,
        sum(case when rs.indicator='t2_i1_2' then coalesce(rs.score,0) else 0 end) t2_i1_2,
        sum(case when rs.indicator='t3_i1' then coalesce(rs.score,0) else 0 end) t3_i1,
        sum(case when rs.indicator='t4_i1' then coalesce(rs.score,0) else 0 end) t4_i1,
        sum(case when rs.indicator='t1_i2' then coalesce(rs.score,0) else 0 end) t1_i2,
        sum(case when rs.indicator='t2_i2' then coalesce(rs.score,0) else 0 end) t2_i2,
        sum(case when rs.indicator='t3_i2_1' then coalesce(rs.score,0) else 0 end) t3_i2_1,
        sum(case when rs.indicator='t3_i2_2' then coalesce(rs.score,0) else 0 end) t3_i2_2,
        sum(case when rs.indicator='t4_i2' then coalesce(rs.score,0) else 0 end) t4_i2,
        sum(case when rs.indicator='t1_i3' then coalesce(rs.score,0) else 0 end) t1_i3,
        sum(case when rs.indicator='t2_i3' then coalesce(rs.score,0) else 0 end) t2_i3,
        sum(case when rs.indicator='t3_i3' then coalesce(rs.score,0) else 0 end) t3_i3,
        sum(case when rs.indicator='t4_i3' then coalesce(rs.score,0) else 0 end) t4_i3,
        sum(case when rs.indicator='t1_i4' then coalesce(rs.score,0) else 0 end) t1_i4,
        sum(case when rs.indicator='t2_i4' then coalesce(rs.score,0) else 0 end) t2_i4,
        sum(case when rs.indicator='t3_i4' then coalesce(rs.score,0) else 0 end) t3_i4,
        sum(case when rs.indicator='t4_i4' then coalesce(rs.score,0) else 0 end) t4_i4,
        sum(case when rs.indicator='t1_i5' then coalesce(rs.score,0) else 0 end) t1_i5,
        sum(case when rs.indicator='t2_i5' then coalesce(rs.score,0) else 0 end) t2_i5,
        sum(case when rs.indicator='t3_i5' then coalesce(rs.score,0) else 0 end) t3_i5,
        sum(case when rs.indicator='t4_i5_1' then coalesce(rs.score,0) else 0 end) t4_i5_1,
        sum(case when rs.indicator='t4_i5_2' then coalesce(rs.score,0) else 0 end) t4_i5_2,
        sum(case when rs.indicator='stress_others_ind' then coalesce(rs.score,0) else 0 end) stress_others_ind,
        sum(case when rs.indicator='stress_self_ind' then coalesce(rs.score,0) else 0 end) stress_self_ind,
        sum(case when rs.indicator='mood_area' then coalesce(rs.score,0) else 0 end) mood_area,
        sum(case when rs.indicator='loneliness_area' then coalesce(rs.score,0) else 0 end) loneliness_area,
        sum(case when rs.indicator='emotional_state_area' then coalesce(rs.score,0) else 0 end) emotional_state_area,
        sum(case when rs.indicator='meaningless_area' then coalesce(rs.score,0) else 0 end) meaningless_area,
        sum(case when rs.indicator='self_efficacy_area' then coalesce(rs.score,0) else 0 end) self_efficacy_area,
        sum(case when rs.indicator='hope_area' then coalesce(rs.score,0) else 0 end) hope_area,
        rank() over (partition by a.activity_id, a.persona_id, date_part('year', a."timestamp") order by a."timestamp" desc ) result_order_desc
    FROM ${warehouse_schema}.xapi_statements a
    JOIN ${warehouse_schema}.tci_surveys s
    ON a.activity_id=s.survey_activity_id
    JOIN ${warehouse_schema}.xapi_statements r
    ON r.registration=a.registration
    JOIN ${warehouse_schema}.tci_response_scores rs
    ON r.statement_id=rs.statement_id
    WHERE a.verb_id='http://adlnet.gov/expapi/verbs/completed'
    GROUP BY a.statement_id, a.registration,a.activity_id, a.persona_id, a."timestamp")
