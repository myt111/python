import trino
from trino import transaction
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import datetime as dt

def trino_execute(sql):
    conn = trino.dbapi.connect(
        host = 'trino.cnio.local',
        port = 80,
        user = 'root',
        catalog = 'hive',
        schema = 'cnio',
        isolation_level = transaction.IsolationLevel.READ_COMMITTED
    )
    cursor = conn.cursor()
    cursor.execute(sql)
    des = cursor.description
    data = cursor.fetchall()
    data = pd.DataFrame(data)

    colname=[]
    for item in des:
        colname.append(item[0])
    data.columns=colname

    cursor.close()
    conn.close()
    return data

def postgre_expert(data):
    engine = create_engine('postgresql://expert-system:YJY_exp#exp502@10.1.77.51:5432/expert-system')
    data.to_sql("pm_4g_workorder",engine,schema="cnio",  index=False, if_exists='append',method='multi')


def get_date():
    today  = dt.datetime.today()
    yestoday = today + dt.timedelta(days=(-1 ))

    yestoday_time = yestoday.strftime('%Y%m%d')

    return yestoday_time


if __name__ == '__main__':
    yestoday  =get_date()
    sql_day  = ''' select 
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
            avg(a.KPI_0000000524) as volte_num
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
            pm_date
        from  hive.cnio.pm_4g
        where cast (pm_date as int) = cast (%s as int)   
    ) a
group by eci,enbid,cellid,pm_date ''' % yestoday
    ## 从trino中提取数据
    data = trino_execute(sql_day)
    ## 数据写入postgresql
    postgre_expert(data)
    print("pm数据入库成功")