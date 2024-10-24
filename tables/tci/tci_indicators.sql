DROP TABLE IF EXISTS ${warehouse_schema}.tci_indicators CASCADE;

CREATE TABLE ${warehouse_schema}.tci_indicators (indicator varchar(20), indicator_description varchar(100), term_threshold integer );

INSERT INTO ${warehouse_schema}.tci_indicators (indicator , indicator_description , term_threshold )
VALUES
('hope_area','Sense of hope/anticipation',1),
('loneliness_area','Sense of loneliness / Weak social support',1),
('meaningless_area','Sense of meaninglessness about life',1),
('mood_area','Risk of low mood',3),
('self_efficacy_area','Perceived self-efficacy in coping',1),
('emotional_state_area','Emotional state entering the new term/year',1);
