drop table if exists data_log.searching_day_log;
create table data_log.searching_day_log(
searching_date timestamp,
table_name text,
latest_data_date date,
recorder_num int8
);