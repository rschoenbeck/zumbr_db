/* Supplier dimension table (dim_supplier) */

DROP TABLE IF EXISTS :tablename;
CREATE TABLE :tablename AS

with product_ranking AS
(
	select
		t.supplierid,
		t.productid,
		rank() over(partition by t.supplierid order by t.total_sold desc) as product_rank
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
product_top_rank as
(
	select
		productid,
		supplierid,
		row_number() over(partition by supplierid order by productid) AS row_ranking
	from product_ranking
	where product_rank = 1
)
select
	s.supplierid,
	s.name,
	tp.productname AS most_profitable_product,
	count(distinct b.suppliertrackid) as unique_tracking_ids,
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
from supplier_vw s
left join businesstxn b on b.supplierid = s.supplierid
left join product p on b.productid = p.productid
left join geography g on b.destinationgeocode = g.ipgeocode
left join product_top_rank ptr on s.supplierid = ptr.supplierid and ptr.row_ranking = 1
left join product tp on ptr.productid = tp.productid
group by s.supplierid, s.name, tp.productname
order by s.supplierid
;