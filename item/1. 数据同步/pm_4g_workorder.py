import trino
from trino import transaction
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import datetime as dt

def trino_conn():
    conn = trino.dbapi.connect(
        host = '133.160.191.113',
        port = 32470,
        user = 'root',
        catalog = 'hive',
        schema = 'cnio',
       # methods='multi',
        isolation_level = transaction.IsolationLevel.READ_COMMITTED
    )
    return conn

def get_pm_data(conn,sql):
    cursor = conn.cursor()
    cursor.execute(sql)
    des = cursor.description
    data = cursor.fetchall()
    
    if len(data):
        data = pd.DataFrame(data)
        colname=[]
        for item in des:
            colname.append(item[0])
        data.columns=colname
    else:
        print("pm数据读取失败")

    cursor.close()
    conn.close()
    return data


def data_upload_expert(data,table_name):
    engine = create_engine('postgresql://expert-system:YJY_exp#exp502@133.160.191.111:5432/expert-system')
    data.to_sql(table_name,engine,schema="cnio",  index=False, if_exists='append',method='multi')

def get_date(day_num):
    today  = dt.datetime.today()
    yestoday = today + dt.timedelta(days=day_num)

    #today_time = today.strftime('%Y%m%d')
    yestoday_date = yestoday.strftime('%Y%m%d')

    return yestoday_date


def get_time_list(days_num):
    now_time = dt.datetime.now()
    date_list = []
    count=0
    while count< (-days_num):
        now_time = now_time + dt.timedelta(days=-1)
        temp_date = now_time.strftime('%Y%m%d')
        if count==0:
            max_date=temp_date
        date_list.append(temp_date)
        min_date = temp_date
        count=count+1

    date_part=[int(min_date),int(max_date)]
    print(date_part)
    return date_list,date_part


def get_loaded_date_list(date_part,expert_db_account):
    # 寤虹珛杩炴帴
    conn = psycopg2.connect(database=expert_db_account["database"], user=expert_db_account["user"], password=expert_db_account["password"],
                            host=expert_db_account["host"], port=expert_db_account["port"])
    cur = conn.cursor()
    cursor = conn.cursor()
    sql = """
    select
	    distinct to_char(trace_date,'yyyymmdd') as trace_date
    from
	    cnio.pm_4g_workorder  tfc
    where
	    cast(to_char(trace_date, 'yyyymmdd') as int) between %s and %s
    """ 
    cursor.execute(sql,date_part)
    data = cursor.fetchall()

    if len(data) != 0:
        data = pd.DataFrame(data)
        data.columns = ["trace_date"]
        data = data["trace_date"].tolist()
        print("数据时间读取完毕")

    else:
        print("数据时间读取失败")
        data = []

    print("uploaded date {0}".format(data))
    cursor.close()
    conn.close()
    return data


def unload_date(date_list, loaded_date_list):
    unload_date_list = []
    for i in range(len(date_list)):
        print("date_list {0}".format(date_list[i]))
        if date_list[i] in loaded_date_list:
            continue
        else:
            unload_date_list.append(date_list[i])
    return unload_date_list

def process(day_num,table_name,conn,expert_db_account):
    date_list,date_part = get_time_list(day_num)
    loaded_date_list=get_loaded_date_list(date_part,expert_db_account)
    unload_date_list  = unload_date(date_list, loaded_date_list)
    print("unload_date_list {0}".format(unload_date_list))
    if len(unload_date_list):
        for i in  range(len(unload_date_list)):
            sql  = ''' select 
            a.eci eci,
            a.enbid enbid,
            a.cellid cellid,
            avg(a.KPI_0000000655) as idletime_prb_noise_power_dbm,
            1-sum(a.KPI_0000000625)/count(a.KPI_0000000625)/3600 as cell_avail_ratio,
            sum(a.KPI_0000000612)/sum(a.KPI_0000000614) as ul_busy_prb_use_ratio,
            sum(a.KPI_0000000613)/sum(a.KPI_0000000615) as dl_busy_prb_use_ratio,
            sum(a.KPI_0000000543)/1000 as ul_traffic_gb,
            sum(a.KPI_0000000547)/1000 as dl_traffic_gb,
            pm_date as trace_date,
            avg(a.KPI_0000000577) as ul_pdcp_throughput,
            avg(a.KPI_0000000578) as dl_pdcp_throughput,
            sum(a.KPI_0000000612) as ul_prb_use_num,
            sum(a.KPI_0000000614) as ul_prb_avail_num,
            sum(a.KPI_0000000613) as dl_prb_use_num,
            sum(a.KPI_0000000615) as dl_prb_avail_num,
            sum(a.KPI_0000000625) as cell_not_avail_duration_ms,
            avg(a.KPI_0000000048) as avg_rrc_num,
            avg(a.KPI_0000000524) as volte_num,
            sum(a.KPI_0000000008)/sum(a.KPI_0000000001) as rrc_est_success_ratio,
            sum(a.KPI_0000000220)/sum(a.KPI_0000000220+a.KPI_0000000227) as ue_context_drop_ratio,
            (sum(a.KPI_0000000298)+sum(a.KPI_0000000300))/(sum(a.KPI_0000000297)+sum(a.KPI_0000000299)) as ho_out_success_ratio,
            avg(a.KPI_0000000652) as avg_cqi,
            sum(a.KPI_0000000157) as erab_est_fail_num_insuf_wl_res
from (
        select 
            (cast(oid as int) * 256 + cast(cellid as int)) as eci,
            (cast(oid as int)) as enbid,
            (cast(cellid as int)) as cellid,
            coalesce(cast (IF(KPI_0000000655='','0',KPI_0000000655)  as double),0) as KPI_0000000655,
            coalesce(cast (IF(KPI_0000000625='','0',KPI_0000000625)  as double),0) as KPI_0000000625,
            coalesce(cast (IF(KPI_0000000612='','0',KPI_0000000612)  as double),0) as KPI_0000000612,
            coalesce(cast (IF(KPI_0000000614='','0',KPI_0000000614)  as double),0) as KPI_0000000614,
            coalesce(cast (IF(KPI_0000000613='','0',KPI_0000000613)  as double),0) as KPI_0000000613,
            coalesce(cast (IF(KPI_0000000615='','0',KPI_0000000615)  as double),0) as KPI_0000000615,
            coalesce(cast (IF(KPI_0000000543='','0',KPI_0000000543)  as double),0) as KPI_0000000543,
            coalesce(cast (IF(KPI_0000000547='','0',KPI_0000000547)  as double),0) as KPI_0000000547,
            coalesce(cast (IF(KPI_0000000577='','0',KPI_0000000577)  as double),0) as KPI_0000000577,
            coalesce(cast (IF(KPI_0000000578='','0',KPI_0000000578)  as double),0) as KPI_0000000578,
            coalesce(cast (IF(KPI_0000000048='','0',KPI_0000000048)  as double),0) as KPI_0000000048,
            coalesce(cast (IF(KPI_0000000524='','0',KPI_0000000524)  as double),0) as KPI_0000000524,
            pm_date,
            coalesce(cast (IF(KPI_0000000008='','0',KPI_0000000008)  as double),0) as KPI_0000000008,
        coalesce(cast (IF(KPI_0000000001='','0',KPI_0000000001)  as double),0) as KPI_0000000001,
        coalesce(cast (IF(KPI_0000000220='','0',KPI_0000000220)  as double),0) as KPI_0000000220,
        coalesce(cast (IF(KPI_0000000227='','0',KPI_0000000227)  as double),0) as KPI_0000000227,
        coalesce(cast (IF(KPI_0000000298='','0',KPI_0000000298)  as double),0) as KPI_0000000298,
        coalesce(cast (IF(KPI_0000000300='','0',KPI_0000000300)  as double),0) as KPI_0000000300,
        coalesce(cast (IF(KPI_0000000297='','0',KPI_0000000297)  as double),0) as KPI_0000000297,
        coalesce(cast (IF(KPI_0000000299='','0',KPI_0000000299)  as double),0) as KPI_0000000299,
        coalesce(cast (IF(KPI_0000000652='','0',KPI_0000000652)  as double),0) as KPI_0000000652,
        coalesce(cast (IF(KPI_0000000157='','0',KPI_0000000157)  as double),0) as KPI_0000000157
        from  hive.cnio.pm_4g
        where cast (pm_date as int) = cast ('%s' as int)  
    ) a
group by eci,enbid,cellid,pm_date '''  % unload_date_list[i]
            data = get_pm_data(conn,sql)
            if len(data)!=0:
                data_upload_expert(data,table_name)
                print("{0}日数据同步成功".format(unload_date_list[i]))
            else:
                print("从trace数据库读取{0}日数据失败，请检查trace数据库中是否有该日task表单".format(unload_date_list[i]))
    else:
        print("数据已是最新，无待同步数据")
        
if __name__ == '__main__':
    conn = trino_conn()
    table_name="pm_4g_workorder"
    #day_num=${delay_days}
    day_num = -7
    expert_db_account = {"database": "expert-system", "user": "expert-system", "password": "YJY_exp#exp502",
                         "host": "133.160.191.111", "port": "5432", "options": "-c search_path=cnio"}
    process(day_num,table_name,conn,expert_db_account)

