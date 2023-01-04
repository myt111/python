import trino
from trino import transaction
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import datetime as dt
from io import StringIO


def get_date():
    today  = dt.datetime.today()
    yestoday = today + dt.timedelta(days=(-1 ))

    today_time = today.strftime('%Y%m%d')
    yestoday_time = yestoday.strftime('%Y-%m-%d')

    return today_time,yestoday_time

def postgre_execute(today_time,yestoday_time):
    # conn = psycopg2.connect(database="sk_data", user="sk", password="CTRO4I!@#=",
    #                         host="133.160.191.108", port="5432")
    conn = psycopg2.connect(database="sk_data", user="sk", password="CTRO4I!@#=",
                            host="133.160.191.108", port="5432")
    # conn = psycopg2.connect(database="trace-tables", user="trace-tables", password="YJY_tra#tra502",
    #                         host="10.1.77.51", port="5432")
    print("Trace Postgres Connection success")

    cur = conn.cursor()
    # 获取taskid
    sql_trace_sk_task  = """
    select taskid from public.sk_task where file_filter =%s and status='完成'
    """
    cur.execute(sql_trace_sk_task, [today_time])
    taskid = cur.fetchall()
    # task_29_lte_ho_snbcell
    ## 如果taskid不为空，
    if len(taskid)==0:
        pass
    else:
        taskid  = pd.DataFrame(taskid)
        taskid.columns = ["taskid"]

        sql = " select  serveci as src_eci,	servenbid as servenbid,	servcellid as servcellid,"+\
	"desteci as dest_eci,destenbid as destenbid,destcellid as destcellid," +\
    "cast(destearfcn as numeric) as destearfcn,cast(destpci as numeric) as destpci,cast(hoouttotalnum as numeric)  as hoouttotalnum,"+\
	"cast(hooutsuccnum as numeric) as hooutsuccnum,cast(hooutprepfailnum as numeric) as hooutprepfailnum,"+\
              "cast(hooutexefailnum as numeric) as hooutexefailnum,cast(hooutsrcreestnum as numeric) as hooutsrcreestnum,"+\
	"cast(hooutdestreestnum as numeric) as hooutdestreestnum,cast(hointotalnum as numeric) as hointotalnum,"+\
	"cast(hoinsuccnum as numeric) as hoinsuccnum,cast(hoinprepfailnum as numeric) as hoinprepfailnum,"+\
	"cast(hoinexefailnum as numeric) as hoinexefailnum,cast(hoindestreestnum as numeric) as hoindestreestnum,"+\
	"cast(hoinsrcreestnum as numeric) as hoinsrcreestnum,cast(intercelldist as numeric) as intercelldist,"+\
	"cast(pciconfcellnum as numeric) as pciconfcellnum,cast(pciconfservid as numeric) as pciconfservid," +\
    "cast(pciconfdestid as numeric) as pciconfdestid,cast(pciconfservnum as numeric) as pciconfservnum," +\
    "cast(pciconfdestnum as numeric) as pciconfdestnum,cast(pciconfminhonum as numeric)  as pciconfminhonum,"+\
	"pciconfsumhonum as pciconfsumhonum,	pciconfsumdestreestnum as pciconfsumdestreestnum,"+\
	" 0 as pciconfsumsrcreestnum,cast( '"+ yestoday_time + " 'as date) as trace_date"+\
    " from public.task_"+str(taskid.loc[0,"taskid"])+"_lte_ho_snbcell"

        cur.execute(sql)
        data = cur.fetchall()
        if len(data) == 0:
            pass
        else:
            data = pd.DataFrame(data)
            des = cur.description
            colname=[]
            for item in des:
                colname.append(item[0])
            data.columns=colname
    cur.close()
    conn.close()
    return data


def postgre_expert(data):
    # engine = create_engine('postgresql://expert-system:YJY_exp#exp502@133.160.191.111:5432/expert-system')
    # data.to_sql("trace_snbcell_ho",engine,schema="cnio",  index=False, if_exists='append',method='multi')
    result = data
    output = StringIO()
    result.to_csv(output, sep='\t', index=False, header=False)
    output1 = output.getvalue()
    conn = psycopg2.connect(host="133.160.191.111", user ="expert-system", password = "YJY_exp#exp502", database = "expert-system",port="5432",options="-c search_path=cnio" )
    cur = conn.cursor()
    cur.copy_from(StringIO(output1), "trace_snbcell_ho",null="")

    print("trace_ho_snbcell sync：success")
    conn.commit()


    cur.close()
    conn.close()

if __name__ == '__main__':
    today_time,yestoday_time = get_date()
    data = postgre_execute(today_time,yestoday_time)
    if len(data)!=0:
        postgre_expert(data)
