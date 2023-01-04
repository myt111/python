import numpy as np
import pandas as pd
from sklearn.cluster import DBSCAN
from  math import radians
from math import tan,atan,acos,sin,cos,asin,sqrt
import datetime
import psycopg2
from sqlalchemy import create_engine

def postgre_expert(data,tablename,schemaname):

    ##结果文件写入工单
    #engine = create_engine('postgresql://expert-system:YJY_exp#exp502@10.1.77.51:5432/expert-system')
    engine = create_engine('postgresql://expert-system:YJY_exp#exp502@133.160.191.111:5432/expert-system')

    #data.to_sql("fault_workorder",engine,schema="cnio",  index=False, if_exists='append')
    data.to_sql(tablename,engine,schema=schemaname, index=False, if_exists='append',method='multi')

def postgre_trace(date_list):
    #conn = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502", host="10.1.77.51", port="5432")
    conn = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502", host="133.160.191.111", port="5432")

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

def data_date_list ():
    ##功能：获得日期清单。程序每周一启动，向前推7天，得到上一周日期清单
    now = datetime.date.today()
    date_list= []
    for i in range(7):
        temp= now + datetime.timedelta(days=(-1-i))
        if temp.month< 10:
            temp_month= "-0"+str(temp.month)
        else:
            temp_month = "-" + str(temp.month)
        if temp.day< 10:
            temp_day= "-0"+str(temp.day)
        else:
            temp_day= "-"+str(temp.day)
        temp= str(temp.year)+temp_month+temp_day

        date_list.append(temp)
    return  date_list

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

def data_input_postgres(data_cluster,date_list):
    print(len(data_cluster))
    data = data_cluster["type"].drop_duplicates()
    data = pd.DataFrame(data)
    data.columns=["rel_id"]
    data["order_id"] = data.index
    data["order_code"] = None
    data["fault_catalog"] = "区域类"
    data["fault_class"] = "覆盖控制"
    data["rel_id"] = None
    data["cell_name"] = ""
    data["lon"] = None
    data["lat"] = None
    data["fault_subclass"] = "弱覆盖"
    data["tuning_action"] = "专家分析"
    data["root_cause"] = "疑似缺站导致"
    data["order_status"] = ""
    data["order_date"] = min(date_list)
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
    ##字段排序
    data = data[["order_id", "order_code", "fault_catalog", "fault_class", "fault_subclass",
                               "cell_name", "lon", "lat", "rel_id", "tuning_action", "root_cause", "order_status",
                               "order_date", "create_time", "rel_id2", "comments", "province", "city",
                               "date_on_list", "expert_analysis", "field_test", "order_sorting", "antenna_feeder_type"]]
    postgre_expert(data,"fault_workorder_r2","cnio")
    print("poorcoverage workorder is inserted into fault_workorder ")

    ##trace_fault_grid
    trace_fault_grid = data_cluster
    trace_fault_grid["fault_id"]= trace_fault_grid.index
    trace_fault_grid.rename(columns={"type":"area_id"},inplace=True)
    trace_fault_grid["fault_type"] = "弱覆盖"
    trace_fault_grid["fault_date"] = min(date_list)

    trace_fault_grid=trace_fault_grid[["fault_id","area_id","gridx","gridy","lon","lat","fault_type","fault_date"]]
    postgre_expert(trace_fault_grid,"trace_fault_grid_r2","cnio")

    print("problem grid is inserted into trace_fault_grid ")


if __name__ == '__main__':

    date_list = data_date_list()
    ## 测试
    date_list = ['2022-09-15','2022-09-16','2022-09-17','2022-09-18','2022-09-19','2022-09-20','2022-09-21']
    trace_poorcoverage_grid=postgre_trace(date_list)

    if  len(trace_poorcoverage_grid)==0:
        print("数据为空，工单生成失败")
    else:
        data_cluster  = dbscan_cluster(trace_poorcoverage_grid)
        #data_cluster.to_csv('coverage_cluster.csv')
	#print(data_cluster.shape[0])
        data_input_postgres(data_cluster,date_list)


