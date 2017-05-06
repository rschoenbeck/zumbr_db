/* Transaction fact table (fact_businesstxn) */

DROP TABLE IF EXISTS :tablename;
CREATE TABLE :tablename AS

select
	b.transactionid,
	b.memberid,
	row_number() over(partition by b.memberid order by transactionid) as member_transaction_number,
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
	p.productname as product_name,
	p.relvalue as product_relvalue,
	sum(p.relvalue) over(partition by b.memberid order by transactionid) as member_cumulative_relvalue,
	b.supplierid,
	s.name as supplier_name,
	b.suppliertrackid,
	coalesce(b.membergeocode, dm.primary_geocode) AS membergeocode,
	gdep.region as departure_region,
	gdep.country as departure_country,
	gdep.state as departure_state,
	gdep.city as departure_city,
	gdest.region as destination_region,
	gdest.country as destination_country,
	gdest.state as destination_state,
	gdest.city as destination_city,
	b.departuregeocode,
	b.destinationgeocode
from businesstxn b
left join product p on b.productid = p.productid
left join dim_member dm on b.memberid = dm.memberid
left join geography g on coalesce(b.membergeocode, dm.primary_geocode) = g.ipgeocode
left join supplier s on b.supplierid = s.supplierid
left join geography gdep on b.departuregeocode = gdep.ipgeocode
left join geography gdest on b.destinationgeocode = gdest.ipgeocode
order by b.transactionid
;