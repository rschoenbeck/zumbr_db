/* Transaction fact table (fact_businesstxn) */

DROP TABLE IF EXISTS :tableschema.:tablename;
CREATE TABLE :tableschema.:tablename AS

select * from businesstxn;