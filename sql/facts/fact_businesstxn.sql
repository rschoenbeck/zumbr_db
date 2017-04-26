/* Transaction fact table (fact_businesstxn) */

DROP TABLE IF EXISTS :tablename;
CREATE TABLE :tablename AS

select
	b.transactionid,
	b.memberid,
	row_number() over(partition by memberid order by transactionid) as member_transaction_number,
	b.usersession,
	b.productitem,
	b.transdatetime,
	to_char(b.transdatetime,'day') as transdayofweek,
	b.clicksourceref,
	case when b.clicksource is null then 'none'
		when b.clicksource in('search form', 'newsletter') then b.clicksource
		else 'erroneous source'
		end as clicksource_type,
	b.clicksourcesend as clicksource_send_time,
	b.businessid,
	b.productid,
	p.relvalue as product_relvalue,
	sum(p.relvalue) over(partition by memberid order by transactionid) as member_cumulative_relvalue,
	b.supplierid,
	b.suppliertrackid,
	b.membergeocode,
	b.departuregeocode,
	b.destinationgeocode
from businesstxn b
left join product p on b.productid = p.productid
order by b.transactionid
;