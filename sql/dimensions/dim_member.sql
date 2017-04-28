/* Member dimension table (dim_member) */

DROP TABLE IF EXISTS :tablename;
CREATE TABLE :tablename AS

with member_dupe_geo as
(
	select
		memberid,
		count(*) as num_geocodes
	from membergeography
	group by memberid having count(*) > 1
),
geo_dupe_rank as 
(
	select
		memberid,
		membergeocode,
		rank() over(partition by memberid order by num_transactions desc) AS transaction_rank,
		row_number() over(partition by memberid) AS geocode_num
	from
	(
		select
			memberid,
			membergeocode,
			count(*) as num_transactions
		from businesstxn
		where memberid in (select memberid from member_dupe_geo)
			and membergeocode is not null
		group by memberid, membergeocode
	) t
),
geo_resolution as
(
	select
		dg.memberid,
		dg.membergeocode
	from geo_dupe_rank dg
	inner join (
		select
			memberid,
			min(geocode_num) as first_geocode_num
		from geo_dupe_rank
		where transaction_rank = 1
		group by memberid
		) dr on dg.memberid = dr.memberid and dg.geocode_num = dr.first_geocode_num
	union
	select
		memberid,
		membergeocode
	from membergeography
	where memberid not in (select memberid from member_dupe_geo)
)
select
	m.memberid,
	gr.membergeocode as primary_geocode,
	g.city as primary_city,
	g.state as primary_state,
	g.country as primary_country,
	g.region as primary_region,
	count(*) as transaction_num,
	sum(p.relvalue) as relvalue_total,
	avg(p.relvalue) as relvalue_transaction_avg,
	count(distinct b.productitem) as productitem_num,
	count(distinct b.productid) as product_num,
	count(distinct b.supplierid) as supplier_num,
	count(distinct b.suppliertrackid) as supplier_track_num,
	count(distinct b.membergeocode) as member_geocode_num,
	count(distinct b.departuregeocode) as departure_num,
	count(distinct b.destinationgeocode) as destination_num,
	count(case when b.clicksource = 'search form' then 1 else null end) as transaction_searchform_num,
	count(case when b.clicksource = 'newsletter' then 1 else null end) as transaction_newsletter_num,
	sum(case when b.clicksource = 'search form' then p.relvalue else 0 end) as transaction_searchform_total,
	sum(case when b.clicksource = 'newsletter' then p.relvalue else 0 end) as transaction_newsletter_total,
	count(distinct b.usersession) as session_num,
	count(distinct case when b.clicksource = 'search form' then b.usersession else null end) as session_searchform_num,
	count(distinct case when b.clicksource = 'newsletter' then b.usersession else null end) as session_newsletter_num
from member m
left join businesstxn b on m.memberid = b.memberid
left join product p on b.productid = p.productid
left join geo_resolution gr on m.memberid = gr.memberid
left join geography g on gr.membergeocode = g.ipgeocode
group by m.memberid, primary_geocode, g.city, g.state, g.country, g.region
;