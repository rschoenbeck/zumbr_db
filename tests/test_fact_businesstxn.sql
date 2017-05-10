with base_difference as
(
	select
		row_count - lag(row_count) over(order by sequence_number) as lagged_row_count_diff,
		relvalue_total - lag(relvalue_total) over(order by sequence_number) as lagged_relvalue_diff
	from
	(
		select 1 as sequence_number, count(*) as row_count, sum(relvalue) as relvalue_total
		from businesstxn b
		left join product p on b.productid = p.productid
		union all
		select 2 as sequence_number, count(*) as row_count, sum(relvalue) as relvalue_total
		from fact_businesstxn fb
		left join product p on fb.productid = p.productid
	) a
)
select ceiling(sum(lagged_row_count_diff) + sum(lagged_relvalue_diff)) AS diff_total from base_difference;
