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
	g.city as primary_city,
	g.state as primary_state,
	g.country as primary_country,
	g.region as primary_region,
	count(*) as transaction_num,
	sum(p.relvalue) as relvalue_total,
	avg(p.relvalue) as relvalue_transaction_avg,
	count(case when clicksource = 'search form' then 1 else null end) as transaction_searchform_num,
	count(case when clicksource = 'newsletter' then 1 else null end) as transaction_newsletter_num,
	sum(case when clicksource = 'search form' then p.relvalue else 0 end) as transaction_searchform_total,
	sum(case when clicksource = 'newsletter' then p.relvalue else 0 end) as transaction_newsletter_total,
	count(distinct usersession) as session_num,
	count(distinct case when clicksource = 'search form' then usersession else null end) as session_searchform_num,
	count(distinct case when clicksource = 'newsletter' then usersession else null end) as session_newsletter_num
from member m
left join businesstxn b on m.memberid = b.memberid
left join product p on b.productid = p.productid
left join geo_resolution gr on m.memberid = gr.memberid
left join geography g on gr.membergeocode = g.ipgeocode
group by m.memberid, g.city, g.state, g.country, g.region
;