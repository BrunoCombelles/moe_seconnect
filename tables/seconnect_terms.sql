DROP TABLE seconnect_dev_warehouse.seconnect_terms;

CREATE TABLE seconnect_dev_warehouse.seconnect_terms 
(term_start_date timestamp,
term_end_date timestamp,
term_description varchar(255),
term_code varchar(10));

INSERT INTO seconnect_dev_warehouse.seconnect_terms 
(term_start_date,term_end_date,term_description,term_code)
VALUES 
('2025-01-02','2025-03-16','Term 1','25T1'),
('2025-03-17','2025-06-22','Term 2','25T2'),
('2025-06-23','2025-09-07','Term 3','25T3'),
('2025-09-07','2025-11-14','Term 4','25T4'),
('2024-01-02','2024-03-16','Term 1','24T1'),
('2024-03-17','2024-06-22','Term 2','24T2'),
('2024-06-23','2024-08-22','Term 3','24T3'),
('2024-08-23','2024-11-14','Term 4','24T4');
