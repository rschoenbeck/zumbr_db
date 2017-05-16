DROP TABLE IF EXISTS :tablename;

CREATE TABLE :tablename
(
	memberid VARCHAR(255),
	productname VARCHAR(255),
	sn_rec_rule_id INT,
	sn_rec_kxReco VARCHAR(255),
	sn_rec_source VARCHAR(255),
	sn_rec_score NUMERIC
);