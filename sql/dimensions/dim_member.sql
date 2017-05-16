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
	pr.sn_rec_kxreco as recommended_product,
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
	count(distinct case when b.clicksource = 'newsletter' then b.usersession else null end) as session_newsletter_num,
	sum(case when b.productid in(1,20,34) then 1 else 0 end) as num_category_flight,
	sum(case when b.productid in(2,18,22) then 1 else 0 end) as num_category_hotel,
	sum(case when b.productid in(3,24,37) then 1 else 0 end) as num_category_car,
	sum(case when b.productid in(31,23) then 1 else 0 end) as num_category_package,
	sum(case when b.productid in(21,28) then 1 else 0 end) as num_category_cruise,
	sum(case when b.productid in(40,41,42) then 1 else 0 end) as num_category_groupon,
	sum(case when b.productid not in(40,41,42,21,28,31,23,3,24,37,2,18,22,1,20,34) then 1 else 0 end) as num_category_other,
	sum(case when b.productid = 1 then 1 else 0 end) as num_flight_search,
	sum(case when b.productid = 2 then 1 else 0 end) as num_hotel_search,
	sum(case when b.productid = 3 then 1 else 0 end) as num_car_search,
	sum(case when b.productid = 12 then 1 else 0 end) as num_misc,
	sum(case when b.productid = 18 then 1 else 0 end) as num_hotel_deal,
	sum(case when b.productid = 20 then 1 else 0 end) as num_flight_referral,
	sum(case when b.productid = 21 then 1 else 0 end) as num_cruise_deal,
	sum(case when b.productid = 22 then 1 else 0 end) as num_hotel_referral,
	sum(case when b.productid = 23 then 1 else 0 end) as num_package_deal,
	sum(case when b.productid = 24 then 1 else 0 end) as num_car_referral,
	sum(case when b.productid = 25 then 1 else 0 end) as num_event_referral,
	sum(case when b.productid = 26 then 1 else 0 end) as num_registration,
	sum(case when b.productid = 28 then 1 else 0 end) as num_cruise_referral,
	sum(case when b.productid = 29 then 1 else 0 end) as num_insurance,
	sum(case when b.productid = 31 then 1 else 0 end) as num_package_referral,
	sum(case when b.productid = 32 then 1 else 0 end) as num_shows_referral,
	sum(case when b.productid = 33 then 1 else 0 end) as num_tour_referral,
	sum(case when b.productid = 34 then 1 else 0 end) as num_flight_coupon,
	sum(case when b.productid = 37 then 1 else 0 end) as num_car_coupon,
	sum(case when b.productid = 40 then 1 else 0 end) as num_groupon_getaways,
	sum(case when b.productid = 41 then 1 else 0 end) as num_groupon_goods,
	sum(case when b.productid = 42 then 1 else 0 end) as num_groupon_deals,
	sum(case when b.productid = 43 then 1 else 0 end) as num_services
from member m
left join businesstxn b on m.memberid = b.memberid
left join product p on b.productid = p.productid
left join geo_resolution gr on m.memberid = gr.memberid
left join geography g on gr.membergeocode = g.ipgeocode
left join top5products pr on pr.memberid = m.memberid and pr.sn_rec_rule_id = 1
group by m.memberid, primary_geocode, g.city, g.state, g.country, g.region, pr.sn_rec_kxreco
;