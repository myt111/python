insert into data_log.expert_day_log
select current_timestamp+interval '8 hours' as searching_date, 'cnio.trace_mr_cell' as table_name, trace_date as latest_data_date ,count(*) as recorder_num from cnio.trace_mr_cell tmrc where trace_date = (select max(trace_date) from cnio.trace_mr_cell tmc) group by trace_date --更新
union all
select current_timestamp+interval '8 hours' as searching_date, 'cnio.trace_mr_grid' as table_name, trace_date as latest_data_date ,count(*) as recorder_num from cnio.trace_mr_grid tmrc where trace_date = (select max(trace_date) from cnio.trace_mr_grid tmc) group by trace_date --更新
union all
select current_timestamp+interval '8 hours' as searching_date, 'cnio.trace_snbcell_ho' as table_name, trace_date as latest_data_date ,count(*) as recorder_num from cnio.trace_snbcell_ho tmrc where trace_date = (select max(trace_date) from cnio.trace_snbcell_ho tmc) group by trace_date --更新
union all
select current_timestamp+interval '8 hours' as searching_date, 'cnio.trace_mr_grid_cell_all' as table_name, trace_date as latest_data_date ,count(*) as recorder_num from cnio.trace_mr_grid_cell_all tmrc where trace_date = (select max(trace_date) from cnio.trace_mr_grid_cell_all tmc) group by trace_date --更新
union all 
select current_timestamp+interval '8 hours' as searching_date, 'cnio.pm_4g_workorder' as table_name, trace_date as latest_data_date ,count(*) as recorder_num from cnio.pm_4g_workorder tmrc where trace_date = (select max(trace_date) from cnio.pm_4g_workorder tmc) group by trace_date ;--更新