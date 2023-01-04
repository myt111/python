import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from  math import radians
from math import tan,atan,acos,sin,cos,asin,sqrt
import datetime
import psycopg2
from sqlalchemy import create_engine
from io import StringIO

def data_upload_stringio(data, table, expert_db_account):
    # 工单数据入库
    output = StringIO()
    data.to_csv(output, sep='$', index=False, header=False)
    output1 = output.getvalue()
    conn = psycopg2.connect(host=expert_db_account["host"], user=expert_db_account["user"],
                            password=expert_db_account["password"], database=expert_db_account["database"],
                            port=expert_db_account["port"], options=expert_db_account["options"])
    cur = conn.cursor()
    cur.copy_from(StringIO(output1), table, sep='$', null='')
    conn.commit()
    cur.close()
    conn.close()
    print("工单入库完毕")

def get_poorcoverage_grid(date_list,expert_db_account):
    conn = psycopg2.connect(database=expert_db_account["database"], user=expert_db_account["user"],
                            password=expert_db_account["password"],host=expert_db_account["host"],
                            port=expert_db_account["port"])

    print("Postgres Connection success")
    cur = conn.cursor()
    # 执行查询
    sql_trace_sk_task  = """
select m.gridx, m.lon,m.gridy,m.lat,((m.servtotalnum-m.servrsrple110)*1.0/m.servtotalnum) as rsrpGe110  from (
select gridx ,gridy,lon,lat,sum(servrsrpdistr0+servrsrpdistr1+servrsrpdistr2+servrsrpdistr3+servrsrpdistr4+servrsrpdistr5+servrsrpdistr6+
servrsrpdistr7+servrsrpdistr8+servrsrpdistr9) as servtotalnum,sum(servrsrpdistr0+servrsrpdistr1+servrsrpdistr2) as servrsrple110 from cnio.trace_mr_grid tmg  
where trace_date =%s or trace_date =%s or trace_date =%s or trace_date =%s 
 or trace_date =%s or trace_date =%s or trace_date =%s group by gridx,gridy,lon,lat ) m  where m.servtotalnum>100 and ((m.servtotalnum-m.servrsrple110)*1.0/m.servtotalnum)<0.7
    """
    # 查询taskid
    cur.execute(sql_trace_sk_task, date_list)
    trace_poorcoverage_grid = cur.fetchall()
    if len(trace_poorcoverage_grid)==0:
        pass
    else:
        trace_poorcoverage_grid = pd.DataFrame(trace_poorcoverage_grid)
        # 表描述
        des = cur.description
        # 获取表头
        colname = []
        for item in des:
            colname.append(item[0])
        trace_poorcoverage_grid.columns = colname

    cur.close()
    conn.close()
    print(trace_poorcoverage_grid.index)
    print("Postgres Connection release")
    return trace_poorcoverage_grid

def data_date_list(day_num):
    ##功能：获得日期清单。程序每周一启动，向前推7天，得到上一周日期清单
    now = datetime.date.today()
    date_list = []
    for i in range(7):
        temp = now + datetime.timedelta(days=(day_num- i))
        temp = temp.strftime('%Y-%m-%d')
        date_list.append(temp)
    return date_list

def dbscan_cluster(data):
    print("弱覆盖聚类开始")
    df = data[['lon', 'lat']]

    kms_per_radian = 6371.0086

    epsilon = 0.05 / kms_per_radian
    ## 密度聚类
    db = DBSCAN(eps=epsilon, min_samples=5, algorithm='ball_tree', metric='haversine').fit(np.radians(df.astype(float)))
    ## 聚类标签
    cluster_labels = db.labels_

    predictionCluster = pd.DataFrame(cluster_labels)
    predictionCluster.columns = ["type"]

    dbPred = pd.merge(data, predictionCluster, how='inner', left_index=True, right_index=True)

    cluster = dbPred[dbPred["type"] != -1]
    print("弱覆盖聚类结束，输出问题清单")

    return cluster

def haversine(lonlat1, lonlat2):
    lat1, lon1 = lonlat1
    lat2, lon2 = lonlat2
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles
    return c * r * 1000

def data_input_postgres(data_cluster,date_list,expert_db_account,fault_workorder_table,fault_grid_table):
    data_mean = data_cluster.groupby(["type"]).mean()
    data_mean = data_mean.reset_index()
    data = data_mean[["type","lon","lat"]]

    data_count = data_cluster.groupby(["type"]).count()
    data_count = data_count.reset_index()
    data_grid_num = data_count[["type", "lon"]]

    data_grid_num_list = data_grid_num.sort_values(by=['lon'], ascending=[False])
    data_grid_num_sublist = data_grid_num_list.head(20)

    workorder_top20 = data_grid_num_sublist[["type"]]

    data = pd.merge(data,workorder_top20,on="type",how="inner")

    data.rename(columns={"type":"rel_id"},inplace=True)
    data.index= range(data.shape[0])


    data["order_id"] = data.index
    data["order_code"] = None
    data["fault_catalog"] = "区域类"
    data["fault_class"] = "覆盖控制"
    #data["rel_id"] = None
    data["cell_name"] = ""
    # data["lon"] = None
    # data["lat"] = None
    data["fault_subclass"] = "弱覆盖"
    data["tuning_action"] = "【专家分析】"
    data["root_cause"] = "-"
    data["order_status"] = "01"
    max_date =datetime.datetime.strptime(max(date_list),'%Y-%m-%d')
    data["order_date"] = max_date.strftime('%Y-%m-%d')
    #data["order_date"] = min(date_list)
    data["create_time"] = datetime.datetime.now()
    data["rel_id2"] = None
    data["date_on_list"] = ""
    data["expert_analysis"] = 1
    data["field_test"] = 1
    data["comments"] = ""
    data["province"] = "河南"
    data["city"] = "郑州"
    data["order_sorting"] = "-"
    data["antenna_feeder_type"] = ""
    data["auto_closed"]=None
    data["tuningaction_gnn"]=None
    data["model_name"]=None
    ##字段排序
    data = data[["order_id", "order_code", "fault_catalog", "fault_class", "fault_subclass",
                               "cell_name", "lon", "lat", "rel_id", "tuning_action", "root_cause", "order_status",
                               "order_date", "create_time", "rel_id2", "comments", "province", "city",
                               "date_on_list", "expert_analysis", "field_test", "order_sorting", "antenna_feeder_type",
                               "auto_closed","tuningaction_gnn","model_name"]]
    data_upload_stringio(data, fault_workorder_table, expert_db_account)
    print("弱覆盖工单同步至fault_workorder表单 ")

    ##trace_fault_grid
    trace_fault_grid = data_cluster
    trace_fault_grid = pd.merge(trace_fault_grid,workorder_top20,on="type",how="inner")

    trace_fault_grid["fault_id"]= trace_fault_grid.index
    trace_fault_grid.rename(columns={"type":"area_id"},inplace=True)
    trace_fault_grid["fault_type"] = "弱覆盖"
    trace_fault_grid["fault_date"] = max(date_list)

    trace_fault_grid=trace_fault_grid[["fault_id","area_id","gridx","gridy","lon","lat","fault_type","fault_date"]]
    data_upload_stringio(trace_fault_grid, fault_grid_table, expert_db_account)

    print("弱覆盖栅格同步至trace_fault_grid表单 ")


if __name__ == '__main__':
    day_num = ${delay_days} ## 周一启动，生成前一周工单

    ## 北京数据库账密
    # expert_db_account = {"database": "expert-system", "user": "expert-system", "password": "YJY_exp#exp502",
    #                      "host": "10.1.77.51", "port": "5432", "options": "-c search_path=cnio"}
    ## 郑州数据库账密
    expert_db_account = {"database":"expert-system", "user":"expert-system","password":"YJY_exp#exp502",
                     "host":"133.160.191.111","port":"5432","options":"-c search_path=cnio"}

    fault_workorder_table = "fault_workorder"
    fault_grid_table = "trace_fault_grid"

    date_list = data_date_list(day_num)
    #date_list = ['2022-11-14', '2022-11-08', '2022-11-09', '2022-11-10', '2022-11-11', '2022-11-12', '2022-11-13']

    trace_poorcoverage_grid=get_poorcoverage_grid(date_list,expert_db_account)

    if  len(trace_poorcoverage_grid)==0:
        print("无弱覆盖栅格，工单生成失败")
    else:
        data_cluster  = dbscan_cluster(trace_poorcoverage_grid)
        data_input_postgres(data_cluster,date_list,expert_db_account,fault_workorder_table,fault_grid_table)


