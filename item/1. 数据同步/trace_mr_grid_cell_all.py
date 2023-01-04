import pandas as pd
import psycopg2
import datetime as dt
from io import StringIO


def get_date(day_num):
    today  = dt.datetime.today()
    yestoday = today + dt.timedelta(days=day_num)

    #today_time = today.strftime('%Y%m%d')
    yestoday_date = yestoday.strftime('%Y%m%d')

    return yestoday_date

def get_trace_data(date,trace_db_account):
    conn = psycopg2.connect(database=trace_db_account["database"], user=trace_db_account["user"], password=trace_db_account["password"],
                            host=trace_db_account["host"], port=trace_db_account["port"])
    cur = conn.cursor()
    # 获取taskid
    sql_trace_sk_task  = """
    select taskid from public.sk_task where to_char(data_begintime,'yyyymmdd') ='%s' and status='完成'
    """ % date
    cur.execute(sql_trace_sk_task)
    taskid = cur.fetchall()

    ## 如果taskid不为空，
    if len(taskid)==0:
        print("trace taskid 获取失败")
        data=[]
    else:
        taskid  = pd.DataFrame(taskid)
        taskid.columns = ["taskid"]

        sql = "select gridx as gridx,  gridy as gridy, lat*1.0/100000 as gridlat,  lon*1.0/100000 as gridlon,"+\
    "eci as eci,   0  as cellindex, pci as pci,  earfcn as earfcn,"+\
    "totalsample as totalsample, servingsample as servingsample,servingrsrp as servingrsrp,"+\
    "servrsrpdistr0 as servrsrpdistr0, servrsrpdistr1 as servrsrpdistr1,"+\
    "servrsrpdistr2 as servrsrpdistr2, servrsrpdistr3 as servrsrpdistr3,"+\
    "servrsrpdistr4 as servrsrpdistr4, servrsrpdistr5 as servrsrpdistr5,"+\
    "servrsrpdistr6 as servrsrpdistr6, servrsrpdistr7 as servrsrpdistr7,"+\
    "servrsrpdistr8 as servrsrpdistr8, servrsrpdistr9 as servrsrpdistr9,"+\
    "servoverlap3sample as servoverlap3sample, servoverlap4sample as servoverlap4sample,"+\
    " 0 as servmod3sample, servovershootingsample as servovershootingsample,"+\
    " 0 as gridtotalsample,  '0' as gridfreqsample,  rsrp as rsrp,"+\
    "bestsample as bestsample,  bestrsrp as bestrsrp, azmrevscellindex as azmrevscellindex,"+\
    "dist as dist,    direct as direct,"+\
    "to_char(to_timestamp(starttime/1000), 'YYYY-MM-DD') as trace_date"+\
    " from public.task_"+str(taskid.loc[0,"taskid"])+"_lte_mr_grid_cell "

        cur.execute(sql,[date])
        data = cur.fetchall()
        if len(data) == 0:
            print("trace taskid 获取成功，但是ho_snbcell表单数据读取失败")
            data = []
        else:
            data = pd.DataFrame(data)
            des = cur.description
            colname=[]
            for item in des:
                colname.append(item[0])
            data.columns=colname
            print("lte_mr_grid_cell_all表单数据读取成功")
    cur.close()
    conn.close()
    return data


def data_upload_expert(data,expert_db_account,table_name):
    # engine = create_engine('postgresql://expert-system:YJY_exp#exp502@133.160.191.111:5432/expert-system')
    # data.to_sql("trace_snbcell_ho",engine,schema="cnio",  index=False, if_exists='append',method='multi')
    result = data
    output = StringIO()
    result.to_csv(output, sep='\t', index=False, header=False)
    output = output.getvalue()
    conn = psycopg2.connect(host=expert_db_account["host"], user =expert_db_account["user"],
                            password = expert_db_account["password"], database = expert_db_account["database"],
                            port=expert_db_account["port"],options=expert_db_account["options"])
    cur = conn.cursor()
    cur.copy_from(StringIO(output), table_name,null="")
    conn.commit()
    cur.close()
    conn.close()
    print("lte_mr_grid_cell_all表单数据同步成功")

def data_type_convert_to_int(data):
    data['gridx'] = data['gridx'].astype(pd.Int64Dtype())
    data['gridy'] = data['gridy'].astype(pd.Int64Dtype())
    data['eci'] = data['eci'].astype(pd.Int64Dtype())
    data['cellindex'] = data['cellindex'].astype(pd.Int64Dtype())
    data['pci'] = data['pci'].astype(pd.Int64Dtype())
    data['earfcn'] = data['earfcn'].astype(pd.Int64Dtype())
    # data['totalsample'] = data['totalsample'].astype(pd.Int64Dtype())
    # data['servingsample'] = data['servingsample'].astype(pd.Int64Dtype())
    # data['servrsrpdistr0'] = data['servrsrpdistr0'].astype(pd.Int64Dtype())
    # data['servrsrpdistr1'] = data['servrsrpdistr1'].astype(pd.Int64Dtype())
    # data['servrsrpdistr2'] = data['servrsrpdistr2'].astype(pd.Int64Dtype())
    # data['servrsrpdistr3'] = data['servrsrpdistr3'].astype(pd.Int64Dtype())
    # data['servrsrpdistr4'] = data['servrsrpdistr4'].astype(pd.Int64Dtype())
    # data['servrsrpdistr5'] = data['servrsrpdistr5'].astype(pd.Int64Dtype())
    # data['servrsrpdistr6'] = data['servrsrpdistr6'].astype(pd.Int64Dtype())
    # data['servrsrpdistr7'] = data['servrsrpdistr7'].astype(pd.Int64Dtype())
    # data['servrsrpdistr8'] = data['servrsrpdistr8'].astype(pd.Int64Dtype())
    # data['servrsrpdistr9'] = data['servrsrpdistr9'].astype(pd.Int64Dtype())
    # data['servoverlap3sample'] = data['servoverlap3sample'].astype(pd.Int64Dtype())
    # data['servoverlap4sample'] = data['servoverlap4sample'].astype(pd.Int64Dtype())
    # data['servmod3sample'] = data['servmod3sample'].astype(pd.Int64Dtype())
    # data['servovershootingsample'] = data['servovershootingsample'].astype(pd.Int64Dtype())
    # data['gridtotalsample'] = data['gridtotalsample'].astype(pd.Int64Dtype())
    # data['gridfreqsample'] = data['gridfreqsample'].astype(pd.Int64Dtype())
    # data['bestsample'] = data['bestsample'].astype(pd.Int64Dtype())
    # data['azmrevscellindex'] = data['azmrevscellindex'].astype(pd.Int64Dtype())
    # data['direct'] = data['direct'].astype(pd.Int64Dtype())

    return data

def get_time_list(days_num):
    now_time = dt.datetime.now()
    date_list = []
    count=0
    while count< (-days_num):
        temp_date = now_time.strftime('%Y%m%d')
        if count==0:
            max_date=temp_date
        date_list.append(temp_date)
        now_time = now_time + dt.timedelta(days=-1)
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
	    cnio.trace_mr_grid_cell_all  tfc
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

    print("upload date {0}".format(data))
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

def process(day_num,table_name,expert_db_account,trace_db_account):
    date_list,date_part = get_time_list(day_num)
    loaded_date_list=get_loaded_date_list(date_part,expert_db_account)
    unload_date_list  = unload_date(date_list, loaded_date_list)
    print("unload_date_list {0}".format(unload_date_list))
    if len(unload_date_list):
        for i in  range(len(unload_date_list)):
            data = get_trace_data(unload_date_list[i],trace_db_account)
            if len(data)!=0:
                data_upload_expert(data,expert_db_account,table_name)
                print("{0}日数据同步成功".format(unload_date_list[i]))
            else:
                print("从trace数据库读取{0}日数据失败，请检查trace数据库中是否有该日task表单".format(unload_date_list[i]))
    else:
        print("数据已是最新，无待同步数据")

if __name__ == '__main__':
    day_num = ${delay_days}  ## 以系统时间为基准，同步 -7天数据
    table_name = "trace_mr_grid_cell_all"

    # 郑州
    trace_db_account = {"database": "sk_data", "user": "sk", "password": "CTRO4I!@#=",
                        "host": "133.160.191.108", "port": "5432"}
    expert_db_account = {"database": "expert-system", "user": "expert-system", "password": "YJY_exp#exp502",
                         "host": "133.160.191.111", "port": "5432", "options": "-c search_path=cnio"}

    # trace_db_account = {"database": "trace-tables", "user": "trace-tables", "password": "YJY_tra#tra502",
    #                     "host": "10.1.77.51", "port": "5432"}
    # expert_db_account = {"database": "expert-system", "user": "expert-system", "password": "YJY_exp#exp502",
    #                      "host": "10.1.77.51", "port": "5432", "options": "-c search_path=cnio"}
    process(day_num,table_name,expert_db_account,trace_db_account)