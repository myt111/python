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

        sql = " select  serveci as src_eci,	servenbid as servenbid,	servcellid as servcellid,"+\
	"desteci as dest_eci,	destenbid as destenbid,	destcellid as destcellid," +\
    "	destearfcn as destearfcn,	destpci as destpci,	hoouttotalnum as hoouttotalnum,"+\
	"hooutsuccnum as hooutsuccnum,	hooutprepfailnum as hooutprepfailnum,"+\
              "hooutexefailnum as hooutexefailnum,hooutsrcreestnum as hooutsrcreestnum,"+\
	"hooutdestreestnum as hooutdestreestnum,	hointotalnum as hointotalnum,"+\
	"hoinsuccnum as hoinsuccnum,	hoinprepfailnum as hoinprepfailnum,"+\
	"hoinexefailnum as hoinexefailnum,	hoindestreestnum as hoindestreestnum,"+\
	"hoinsrcreestnum as hoinsrcreestnum,intercelldist as intercelldist,"+\
	"pciconfcellnum as pciconfcellnum,	pciconfservid as pciconfservid," +\
    "pciconfdestid as pciconfdestid,	pciconfservnum as pciconfservnum," +\
    "	pciconfdestnum as pciconfdestnum,	pciconfminhonum as pciconfminhonum,"+\
	"pciconfsumhonum as pciconfsumhonum,	pciconfsumdestreestnum as pciconfsumdestreestnum,"+\
	" 0 as pciconfsumsrcreestnum,%s as trace_date "+\
    "from public.task_"+str(taskid.loc[0,"taskid"])+"_lte_ho_snbcell "

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
            print("ho_snbcell表单数据读取成功")
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
    print("trace_ho_snbcell表单数据同步成功")

def data_type_convert_to_int(data):
    data['src_eci'] = data['src_eci'].astype(pd.Int64Dtype())
    data['servenbid'] = data['servenbid'].astype(pd.Int64Dtype())
    data['servcellid'] = data['servcellid'].astype(pd.Int64Dtype())
    data['dest_eci'] = data['dest_eci'].astype(pd.Int64Dtype())
    data['destenbid'] = data['destenbid'].astype(pd.Int64Dtype())
    data['destcellid'] = data['destcellid'].astype(pd.Int64Dtype())
    data['destearfcn'] = data['destearfcn'].astype(pd.Int64Dtype())
    data['destpci'] = data['destpci'].astype(pd.Int64Dtype())
    data['hoouttotalnum'] = data['hoouttotalnum'].astype(pd.Int64Dtype())
    data['hooutsuccnum'] = data['hooutsuccnum'].astype(pd.Int64Dtype())
    data['hooutprepfailnum'] = data['hooutprepfailnum'].astype(pd.Int64Dtype())
    data['hooutexefailnum'] = data['hooutexefailnum'].astype(pd.Int64Dtype())
    data['hooutsrcreestnum'] = data['hooutsrcreestnum'].astype(pd.Int64Dtype())
    data['hooutdestreestnum'] = data['hooutdestreestnum'].astype(pd.Int64Dtype())
    data['hointotalnum'] = data['hointotalnum'].astype(pd.Int64Dtype())
    data['hoinsuccnum'] = data['hoinsuccnum'].astype(pd.Int64Dtype())
    data['hoinprepfailnum'] = data['hoinprepfailnum'].astype(pd.Int64Dtype())
    data['hoinexefailnum'] = data['hoinexefailnum'].astype(pd.Int64Dtype())
    data['hoindestreestnum'] = data['hoindestreestnum'].astype(pd.Int64Dtype())
    data['hoinsrcreestnum'] = data['hoinsrcreestnum'].astype(pd.Int64Dtype())
    data['intercelldist'] = data['intercelldist'].astype(pd.Int64Dtype())
    data['pciconfcellnum'] = data['pciconfcellnum'].astype(pd.Int64Dtype())
    data['pciconfservid'] = data['pciconfservid'].astype(pd.Int64Dtype())
    data['pciconfdestid'] = data['pciconfdestid'].astype(pd.Int64Dtype())
    data['pciconfservnum'] = data['pciconfservnum'].astype(pd.Int64Dtype())
    data['pciconfdestnum'] = data['pciconfdestnum'].astype(pd.Int64Dtype())
    data['pciconfminhonum'] = data['pciconfminhonum'].astype(pd.Int64Dtype())
    #data['pciconfsumhonum'] = data['pciconfsumhonum'].astype(pd.Int64Dtype())
    #data['pciconfsumdestreestnum'] = data['pciconfsumdestreestnum'].astype(pd.Int64Dtype())
    #data['pciconfsumsrcreestnum'] = data['pciconfsumsrcreestnum'].astype(pd.Int64Dtype())

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
	    cnio.trace_snbcell_ho  tfc
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
                data=data_type_convert_to_int(data)
                data_upload_expert(data,expert_db_account,table_name)
                print("{0}日数据同步成功".format(unload_date_list[i]))
            else:
                print("从trace数据库读取{0}日数据失败，请检查trace数据库中是否有该日task表单".format(unload_date_list[i]))
    else:
        print("数据已是最新，无待同步数据")
        
if __name__ == '__main__':
    day_num = -7  ## 以系统时间为基准，同步 -7天数据
    table_name = "trace_snbcell_ho"

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

