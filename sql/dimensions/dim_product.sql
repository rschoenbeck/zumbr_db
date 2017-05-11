/* Prouct dimension table (dim_product) */

DROP TABLE IF EXISTS :tablename;
CREATE TABLE :tablename AS

with supplier_ranking AS
(
	select
		t.productid,
		t.supplierid,
		rank() over(partition by t.productid order by t.total_sold desc) as supplier_rank
	from 
	(
		select
			b.supplierid,
			b.productid,
			sum(p.relvalue) AS total_sold
		from businesstxn b
		inner join product p on b.productid = p.productid
		group by b.supplierid, b.productid
	) t

),
supplier_top_rank as
(
	select
		productid,
		supplierid,
		row_number() over(partition by productid order by supplierid) AS row_ranking
	from supplier_ranking
	where supplier_rank = 1
)
select
	p.productid,
	p.productname,
	case when p.productid in(1,20,34) then 'Flight'
		when p.productid in (2,18,22) then 'Hotel'
		when p.productid in (3,24,37) then 'Car'
		when p.productid in (31,23) then 'Package'
		when p.productid in (21,28) then 'Cruise'
		when p.productid in (40,41,42) then 'Groupon'
		else 'Other'
		end as product_category,
	p.relvalue,
	ts.name AS top_supplier,
	sum(p.relvalue) AS total_relvalue_sold,
	count(*) AS total_number_sold,
	sum(case when b.clicksource = 'newsletter' then p.relvalue else 0 end) AS total_sold_via_newsletter,
	sum(case when b.clicksource = 'search form' then p.relvalue else 0 end) AS total_sold_via_searchform,
	sum(case when g.region = 'Oceania' then p.relvalue else 0 end) AS total_sold_destination_oceania,
	sum(case when g.region = 'Asia' then p.relvalue else 0 end) AS total_sold_destination_asia,
	sum(case when g.region = 'Southeast Asia' then p.relvalue else 0 end) AS total_sold_destination_se_asia,
	sum(case when g.region = 'North America Other' then p.relvalue else 0 end) AS total_sold_destination_north_america_other,
	sum(case when g.region = 'North America U.S.' then p.relvalue else 0 end) AS total_sold_destination_north_america_us,
	sum(case when g.region = 'South America' then p.relvalue else 0 end) AS total_sold_destination_south_america,
	sum(case when g.region = 'Europe' then p.relvalue else 0 end) AS total_sold_destination_europe
from product p
left join businesstxn b on b.productid = p.productid
left join geography g on b.destinationgeocode = g.ipgeocode
left join supplier_top_rank str on p.productid = str.productid and str.row_ranking = 1
left join supplier_vw ts on str.supplierid = ts.supplierid
group by p.productid, p.productname, p.relvalue, ts.name
order by p.productid
;