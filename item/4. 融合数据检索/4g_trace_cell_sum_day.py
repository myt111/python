import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import datetime as dt
from io import StringIO

def get_date(day_num):
    today  = dt.datetime.today()
    yestoday = today + dt.timedelta(days=day_num)
    yestoday_date=yestoday.strftime('%Y%m%d')
    yestoday_date_ = yestoday.strftime('%Y-%m-%d')
    return yestoday_date,yestoday_date_

def get_trace_data(date,trace_db_account):

    conn = psycopg2.connect(database=trace_db_account["database"], user=trace_db_account["user"], password=trace_db_account["password"],
                            host=trace_db_account["host"], port=trace_db_account["port"])

    cur = conn.cursor()
    # 获取taskid
    sql_trace_sk_task  = """
    select taskid from public.sk_task where to_char(data_begintime,'yyyymmdd') =%s and status='完成'
    """
    cur.execute(sql_trace_sk_task, [date[0]])
    taskid = cur.fetchall()

    ## 如果taskid不为空，
    if len(taskid)==0:
        print("从trace数据库读取{0}日对应taskid失败".format(date[0]))
        data=[]
    else:
        taskid  = pd.DataFrame(taskid)
        taskid.columns = ["taskid"]

        sql = """
                select cellname	,	
        	cellindex	,
        	pci	,
            earfcn	,	
            eci	,	
        	enbid	,	
        	cellid	,	
        	celllon	,	
        	celllat	,	
        	hbwd	,	
        	azimuth	,	
        	height	,	
        	indoor	,	
        	etilt	,	
        	mtilt	,	
        	vendor	,	
        	province	,
        	city	,	
        	district	,	
        	scene	,	
        	covertype	,	
        	siteavgdist	,	
        	celltype	,	
        	indoorleak	,	
        	overshooting	,	
        	insuffcover	,	
        	azimuthcheck	,	
        	locationerr	,	
        	maxdist	,
        	mindist	,	
        	avgdist	,	
        	totalmrsample_all	,	
        	rsrp_all	,	
        	bestsample_all	,
        	bestrsrp_all	,
        	servingsample_all	,	
        	servingrsrp_all	,	
        	overlap3sample_all	,	
        	overlap4sample_all	,	
        	overshootingsample_all	,	
        	poorcoveragesample_all	,	
        	servdeltam3sample_all	,	
        	servnotbestsample_all	,
        	servdelta3sample_all	,	
        	interferencesample_all	,	
        	interfdelta3sample_all	,	
        	mod3sample_all	,	
        	totalmrsample	,	
        	rsrp	,	
        	bestsample	,	
        	bestrsrp	,	
        	servingsample	,	
        	servingrsrp	,	
        	overshootingsample	,	
        	poorcoveragesample	,	
        	overlap3sample	,	
        	overlap4sample	,	
        	servdeltam3sample	,	
        	servnotbestsample	,	
        	servdelta3sample	,	
        	interferencesample	,	
        	interfdelta3sample	,
        	mod3sample	,	
        	totalgridnum	,	
        	servingnum	,	
        	overshootingnum	,	
        	poorcoveragenum	,	
        	overlap3num	,	
        	overlap4num	,	
        	servdelta3mnum	,	
        	servnotbestnum	,	
        	servdelta3num	,	
        	interferencenum	,	
        	interfdelta3num	,	
        	conntotalnum	,	
        	connreqnum	,	
        	connfailnum	,	
        	conndropnum	,	
        	noendmsgnum	,	
        	reestreqnum	,	
        	reestendnum	,	
        	redirgunum	,	
        	redirnrnum	,	
        	hooutgunum	,	
        	hooutnrnum	,	
        	uecapnrnum	,	
        	nbcellnum	,	
        	hoouttotalnum	,	
        	hooutsuccnum	,	
        	hooutprepfailnum	,	
        	hooutexefailnum	,	
        	hooutsrcreestnum	,	
        	hooutdestreestnum	,	
        	servcellnum	,	
        	hointotalnum	,	
        	hoinsuccnum	,	
        	hoinprepfailnum	,	
        	hoinexefailnum	,	
        	hoinsrcreestnum	,	
        	hoindestreestnum	,	
        	pciconfservnum	,	
        	pciconfhoouttotalnum	,	
        	pciconfhooutsuccnum	,	
        	pciconfhooutprepfailnum	,	
        	pciconfhooutexefailnum	,	
        	pciconfhooutsrcreestnum	,	
        	pciconfhooutdestreestnum	,	
        	pciconfdestnum	,	
        	pciconfhointotalnum	,	
        	pciconfhoinsuccnum	,	
        	pciconfhoinprepfailnum	,	
        	pciconfhointexefailnum	,	
        	pciconfhoinsrcreestnum	,	
        	pciconfhoindestreestnum	,	
        	nbcfgcellnum	,	
        	interfercellnum	,	
        	mode3cellnum	,	
        	(case when ultotalmodtbs>0 then (ul16qam/ultotalmodtbs)*100 else NULL end) as  ul16qamdistr	,	
        	(case when dltotalmodtbs>0 then (dl64qam/dltotalmodtbs)*100	else NULL end) as dl64qamdistr	,
        	ul16qam	,	
        	ultotalmodtbs	,	
        	dl64qam	,	
        	dltotalmodtbs	,	
        	(case when ultotalprb>0 then (uldrbprb/ultotalprb)*100 else NULL end)  as uldrbprbusage	,	
        	(case when dltotalprb>0 then (dldrbprb/dltotalprb)*100 else NULL end)  as dldrbprbusage	,	
        	uldrbprb	,	
        	dldrbprb	,	
        	ultotalprb	,	
        	dltotalprb	,	
        	(case when ultotaltbs>0 then (ulretxtb/ultotaltbs)*100  else NULL end) as ulretxrate	,	
        	(case when dltotaltbs>0 then (dlretxrate/dltotaltbs)*100  else NULL end)	 as  dlretxrate	, 
        	ulretxtb	,	
        	dlretxtb	,	
        	ultotaltbs	,	
        	dltotaltbs	,	
        	servdelta3bytesdistr	,	
        	servdelta3ulbytesdistr	,	
        	servdelta3dlbytesdistr	,	
        	intfdelta3bytesdistr	,	
        	intfdelta3ulbytesdistr	,	
        	intfdelta3dlbytesdistr	,	
        	bytesgrade	,	
        	avgbytes	,	
        	ulbytes	,	
        	dlbytes	,	
        	totalbytes	,	
        	(case when totalcqinum>0 then (1-(cqi0_6/totalcqinum))*100 else NULL end) as cqil7distr	,	
        	cqi0_6	,	
        	totalcqinum,	
            %s  as data_date		
         from public.task_%s_lte_cell_sum 
        """

        cur.execute(sql,[date[1],int(taskid.loc[0,"taskid"])])

        data = cur.fetchall()
        if len(data) == 0:
            print("从trace数据库读取{0}日数据失败".format(date[1]))
            data=[]
        else:
            data = pd.DataFrame(data)
            des = cur.description
            colname=[]
            for item in des:
                colname.append(item[0])
            data.columns=colname
            print("日期1：{0}".format(data.loc[0,"data_date"]))
            print("从trace数据库{0}日数据读取成功".format(date[1]))
    cur.close()
    conn.close()
    return data

def data_upload_expert(data,expert_db_account,table_name):
    print("日期2：{0}".format(data.loc[0,"data_date"]))

    database=expert_db_account["database"]
    user=expert_db_account["user"]
    password = expert_db_account["password"]
    host=expert_db_account["host"]
    port=expert_db_account["port"]
    engine_str = 'postgresql://'+user+":"+password+"@"+host+":"+port+"/"+database
    engine = create_engine(engine_str)
    data.to_sql(table_name,engine,schema="searching", index=False, if_exists='append',method='multi')
    # output = StringIO()
    # data.to_csv(output, sep='\t', index=False, header=False)
    # output1 = output.getvalue()
    # conn = psycopg2.connect(host="10.1.77.51", user="expert-system", password="YJY_exp#exp502",
    #                         database="expert-system", port="5432",options="-c search_path=searching")
    # cur = conn.cursor()
    # cur.copy_from(StringIO(output1), "trace_lte_cell_sum_day")
    # conn.commit()
    # cur.close()
    # conn.close()
    # print("trace_lte_cell_sum_day sync: success")

def get_time_list(days_num):
    now_time = dt.datetime.now()
    date_list = []
    count=0
    while count< (-days_num):
        now_time = now_time + dt.timedelta(days=-1)
        temp_date = now_time.strftime('%Y%m%d')
        temp_date_ = now_time.strftime('%Y-%m-%d')
        if count==0:
            max_date=temp_date
        date_list.append([temp_date,temp_date_])
        min_date = temp_date
        count=count+1
    date_part=[int(min_date),int(max_date)]
    print("date_part {0}".format(date_part))
    return date_list,date_part


def get_loaded_date_list(date_part,expert_db_account):
    # 寤虹珛杩炴帴
    conn = psycopg2.connect(database=expert_db_account["database"], user=expert_db_account["user"], password=expert_db_account["password"],
                            host=expert_db_account["host"], port=expert_db_account["port"])
    cur = conn.cursor()
    cursor = conn.cursor()
    sql = """
select
	distinct to_char(cast(data_date as date), 'yyyymmdd') as data_date 
from
	searching.trace_lte_cell_sum_day  pgw  
where
	cast(to_char(cast(data_date as date), 'yyyymmdd') as int) between %s and %s
    """ 
    cursor.execute(sql,date_part)
    data = cursor.fetchall()

    if len(data) != 0:
        data = pd.DataFrame(data)
        data.columns = ["data_date"]
        data = data["data_date"].tolist()
        print("数据时间读取完毕")

    else:
        print("数据时间读取失败")
        data = []

    print("upload date {0}".format(data))
    cursor.close()
    conn.close()
    return data


def unload_date(date_list, loaded_date_list):
    unload_date_list = []
    for i in range(len(date_list)):
        print("date_list {0}".format(date_list[i]))
        if date_list[i][0] in loaded_date_list:
            continue
        else:
            unload_date_list.append(date_list[i])
    return unload_date_list

def process(day_num,table_name,expert_db_account,trace_db_account):
    date_list,date_part= get_time_list(day_num)
    loaded_date_list=get_loaded_date_list(date_part,expert_db_account)
    unload_date_list  = unload_date(date_list, loaded_date_list)
    print("unload_date_list {0}".format(unload_date_list))
    if len(unload_date_list):
        for i in  range(len(unload_date_list)):
            data = get_trace_data(unload_date_list[i],trace_db_account)
            if len(data)!=0:
                data_upload_expert(data,expert_db_account,table_name)
                print("{0}日数据同步成功".format(unload_date_list[i][1]))
            else:
                print("从trace数据库读取{0}日数据失败，请检查trace数据库中是否有该日task表单".format(unload_date_list[i][1]))
    else:
        print("数据已是最新，无待同步数据")
    
if __name__ == '__main__':
    day_num = ${delay_days}
    table_name = "trace_lte_cell_sum_day"

    # 郑州数据库连接
    trace_db_account = {"database": "sk_data", "user": "sk", "password": "CTRO4I!@#=",
                        "host": "133.160.191.108", "port": "5432"}
    expert_db_account = {"database": "expert-system", "user": "expert-system", "password": "YJY_exp#exp502",
                         "host": "133.160.191.111", "port": "5432", "options": "-c search_path=cnio"}
    # 北京数据库连接
    # trace_db_account = {"database": "trace-tables", "user": "trace-tables", "password": "YJY_tra#tra502",
    #                     "host": "10.1.77.51", "port": "5432"}
    # expert_db_account = {"database": "expert-system", "user": "expert-system", "password": "YJY_exp#exp502",
    #                      "host": "10.1.77.51", "port": "5432", "options": "-c search_path=cnio"}
    process(day_num,table_name,expert_db_account,trace_db_account)


