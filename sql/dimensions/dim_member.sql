/* Member dimension table (dim_member) */

DROP TABLE IF EXISTS :tableschema.:tablename;
CREATE TABLE :tableschema.:tablename AS

select * from member;
