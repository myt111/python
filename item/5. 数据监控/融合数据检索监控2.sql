insert
	into
	data_log.searching_day_log
select
	current_timestamp as searching_date,
	'searching.trace_lte_cell_sum_day' as table_name,
	cast(data_date as date) as latest_data_date ,
	count(*) as recorder_num
from
	searching.trace_lte_cell_sum_day tlcsd
where
	data_date = (
	select
		max(data_date)
	from
		searching.trace_lte_cell_sum_day tmc)
group by
	data_date
	--更新
union all 
select
	current_timestamp as searching_date,
	'searching.trace_lte_cell_sum_week' as table_name,
	cast(data_date as date) as latest_data_date ,
	count(*) as recorder_num
from
	searching.trace_lte_cell_sum_week tlcsd
where
	data_date = (
	select
		max(data_date)
	from
		searching.trace_lte_cell_sum_week tmc)
group by
	data_date ;--更新