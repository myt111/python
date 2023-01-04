import pandas as pd
import psycopg2
import datetime
from sqlalchemy import create_engine
import datetime as dt
from io import StringIO  



def get_taskid(delay_date,trace_db_account):
    conn = psycopg2.connect(database=trace_db_account["database"], user=trace_db_account["user"],
                            password=trace_db_account["password"], host=trace_db_account["host"],
                            port=trace_db_account["port"])
    cur = conn.cursor()
    # 执行查询

    sql_trace_sk_task = """
   select taskid,to_char(data_begintime, 'yyyymmdd') as file_filter  from public.sk_task where to_char(data_begintime, 'yyyymmdd') = '%s'
   """  % delay_date
    # 获取taskid 45
    # 获取taskid
    cur.execute(sql_trace_sk_task)
    taskid = cur.fetchall()
    if len(taskid) != 0:
        taskid = pd.DataFrame(taskid)
        taskid.columns = ["taskid", "file_filter"]

    print("taskid 获取完毕")
    cur.close()
    conn.close()
    return taskid

def drop_table(taskid, trace_db_account):
    conn = psycopg2.connect(database=trace_db_account["database"], user=trace_db_account["user"],
                            password=trace_db_account["password"], host=trace_db_account["host"],
                            port=trace_db_account["port"])
    cur = conn.cursor()
    # 初始赋值
    id_list = []
    for i in range(43):
        id_list.append(int(taskid.loc[0,"taskid"]))
    print("id_list {0}".format(id_list))
    sql  = """
    drop table if exists public.task_%s_imsi;
	drop table if exists public.task_%s_lte_call;
	drop table if exists public.task_%s_lte_cell_sum;
	drop table if exists public.task_%s_lte_conn;
	drop table if exists public.task_%s_lte_conn_cell_servtype;
	drop table if exists public.task_%s_lte_conn_gis;
	drop table if exists public.task_%s_lte_conn_grid_cell_servtype;
	drop table if exists public.task_%s_lte_erab;
	drop table if exists public.task_%s_lte_erab_cell_qci;
	drop table if exists public.task_%s_lte_erab_grid_cell_qci;
	drop table if exists public.task_%s_lte_event;
	drop table if exists public.task_%s_lte_ho_cell;
	drop table if exists public.task_%s_lte_ho_pciconf;
	drop table if exists public.task_%s_lte_ho_snbcell;
	drop table if exists public.task_%s_lte_hoout_snbcell;
	drop table if exists public.task_%s_lte_mr_cell;
	drop table if exists public.task_%s_lte_mr_cell_total;
	drop table if exists public.task_%s_lte_mr_grid;
	drop table if exists public.task_%s_lte_mr_grid_cell;
	drop table if exists public.task_%s_lte_mr_grid_interfcell;
	drop table if exists public.task_%s_lte_mr_grid_snbcell;
	drop table if exists public.task_%s_lte_mr_grid_snbcell_total;
	drop table if exists public.task_%s_lte_mr_overlaparea;
	drop table if exists public.task_%s_lte_mr_poorcoveragearea;
	drop table if exists public.task_%s_lte_mr_snbcell;
	drop table if exists public.task_%s_lte_mr_snbcell_total;
	drop table if exists public.task_%s_lte_mr_user;
	drop table if exists public.task_%s_lte_mr_userconn;
	drop table if exists public.task_%s_lte_rrc_ueinforesp;
	drop table if exists public.task_%s_lte_snbcell_sum;
	drop table if exists public.task_%s_lte_uuextend_cell;
	drop table if exists public.task_%s_lte_uuextend_cellhour;
	drop table if exists public.task_%s_lte_uuextend_user;
	drop table if exists public.task_%s_lte_uuextend_user_cell;
	drop table if exists public.task_%s_lte_uuextend_user_grid_snbcell_total;
	drop table if exists public.task_%s_lte_uuextend_user_snbcell;
	drop table if exists public.task_%s_lte_uuextend_userconn;
	drop table if exists public.task_%s_siteinfo;
	drop table if exists public.task_%s_siteinfo_err;
	drop table if exists public.task_%s_siteinfo_org;
	drop table if exists public.task_%s_sk_file_id;
	drop table if exists public.task_%s_tasklog;
	drop table if exists public.task_%s_video;
    """
    cur.execute(sql,id_list)
    conn.commit()
    cur.close
    conn.close

    print("trace表单删除成功")


def get_date(day_num):
    today = dt.datetime.today()
    delay_date = today + dt.timedelta(days=(day_num))
    delay_date_str = delay_date.strftime('%Y%m%d')

    return delay_date_str

if __name__ == '__main__':
    
    expert_db_account = {"database":"sk_data", "user":"sk","password":"CTRO4I!@#=",
                     "host":"133.160.191.108","port":"5432","options":"-c search_path=public"}
                     
    day_num= ${delay_days}
    delay_date=get_date(day_num)
    taskid = get_taskid(delay_date,expert_db_account)
    if len(taskid):
        drop_table(taskid,expert_db_account)
    else:
        print("taskid获取失败")