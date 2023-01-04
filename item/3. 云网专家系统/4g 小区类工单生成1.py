import pandas as pd
import psycopg2
import datetime
from sqlalchemy import create_engine
import datetime as dt
from io import StringIO


def data_upload_tosql(data, tablename, schemaname):
    # 工单数据入库
    engine = create_engine('postgresql://expert-system:YJY_exp#exp502@133.160.191.111:5432/expert-system')
    data.to_sql(tablename, engine, schema=schemaname, index=False, if_exists='append', method='multi')


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
    print("小区类工单入库完毕")


def date_task(day_num):
    today = dt.datetime.today()
    date_list = []
    for i in range(7):
        temp = today + dt.timedelta(days=(day_num-i))
        temp_date = temp.strftime('%Y%m%d')
        # yestoday_time_v = yestoday.strftime('%Y-%m-%d')
        date_list.append(temp_date)
    #date_list = ['20221114', '20221108', '20221109', '20221110', '20221111', '20221112', '20221113']

    return date_list


def get_taskid(date_list, trace_db_account):
    conn = psycopg2.connect(database=trace_db_account["database"], user=trace_db_account["user"],
                            password=trace_db_account["password"], host=trace_db_account["host"],
                            port=trace_db_account["port"])
    cur = conn.cursor()
    # 执行查询
    sql_trace_sk_task = """
    select taskid,to_char(data_begintime, 'yyyymmdd') as file_filter  from public.sk_task where (to_char(data_begintime, 'yyyymmdd') =%s and status='完成')  or   (to_char(data_begintime, 'yyyymmdd') =%s and status='完成') or
    (to_char(data_begintime, 'yyyymmdd') =%s and status='完成') or (to_char(data_begintime, 'yyyymmdd') =%s and status='完成') or (to_char(data_begintime, 'yyyymmdd') =%s and status='完成') or
    (to_char(data_begintime, 'yyyymmdd') =%s and status='完成') or (to_char(data_begintime, 'yyyymmdd') =%s and status='完成')
    """
    # 获取taskid
    cur.execute(sql_trace_sk_task, date_list)
    taskid = cur.fetchall()
    if len(taskid) != 0:
        taskid = pd.DataFrame(taskid)
        taskid.columns = ["taskid", "file_filter"]

    # 临时指定
    #taskid = pd.DataFrame({"taskid": [43], "file_filter": ["20221115"]})
    print("taskid 获取完毕")
    cur.close()
    conn.close()
    return taskid


## 天线接反工单
def antenna_reverse(date_list,taskid, threshhold_day, trace_db_account):
    conn = psycopg2.connect(database=trace_db_account["database"], user=trace_db_account["user"],
                            password=trace_db_account["password"], host=trace_db_account["host"],
                            port=trace_db_account["port"])
    cur = conn.cursor()
    # 初始赋值
    antenna_reverse_data = []
    for i in range(taskid.shape[0]):
        data_query_sql = "(select cellname,eci,celllon,celllat from public.task_" + str(taskid.loc[i, "taskid"]) + \
                         "_lte_cell_sum where azimuthcheck=3 and indoor =0) "
        cur.execute(data_query_sql)
        antenna_reverse = cur.fetchall()
        if len(antenna_reverse) != 0:
            antenna_reverse = pd.DataFrame(antenna_reverse)
            # 表描述
            des = cur.description
            # 获取表头
            colname = []
            for item in des:
                colname.append(item[0])
            antenna_reverse.columns = colname
            antenna_reverse["trace_date"] = taskid.loc[i, "file_filter"]

            if i == 0:
                antenna_reverse_data = antenna_reverse
            else:
                antenna_reverse_data = pd.concat([antenna_reverse_data, antenna_reverse])

    if len(antenna_reverse_data) != 0:
        antenna_reverse_data.index = range(antenna_reverse_data.shape[0])
        ##计算出镜次数
        antenna_r_d = antenna_reverse_data.groupby([antenna_reverse_data["cellname"],
                                                    antenna_reverse_data["eci"], antenna_reverse_data["celllon"],
                                                    antenna_reverse_data["celllat"]])["trace_date"].count()
        antenna_r_d = antenna_r_d.reset_index()
        antenna_r_d = antenna_r_d[antenna_r_d["trace_date"] >= threshhold_day]  ###实际数据应为4
        antenna_r_d["date_on_list"] = ""
        antenna_r_d.index = range(antenna_r_d.shape[0])
        for i in range(antenna_r_d.shape[0]):
            for j in range(antenna_reverse_data.shape[0]):
                if antenna_r_d.loc[i, "eci"] == antenna_reverse_data.loc[j, "eci"]:
                    if antenna_r_d.loc[i, "date_on_list"] == "":
                        antenna_r_d.loc[i, "date_on_list"] = antenna_reverse_data.loc[j, "trace_date"] + "=1"
                    else:
                        antenna_r_d.loc[i, "date_on_list"] = antenna_r_d.loc[i, "date_on_list"] + "," + \
                                                             antenna_reverse_data.loc[j, "trace_date"] + "=1"
        for i in range(antenna_r_d.shape[0]):
            for j in range(taskid.shape[0]):
                if taskid.loc[j, "file_filter"] not in antenna_r_d.loc[i, "date_on_list"]:
                    antenna_r_d.loc[i, "date_on_list"] = antenna_r_d.loc[i, "date_on_list"] + "," + taskid.loc[
                        j, "file_filter"] + "=0"
        antenna_r_d.rename(columns={"eci": "rel_id", "celllon": "lon", "celllat": "lat", "cellname": "cell_name"},
                           inplace=True)
        ##字段填充
        antenna_r_d["order_id"] = antenna_r_d.index
        antenna_r_d["order_code"] = None
        antenna_r_d["fault_catalog"] = "小区类"
        antenna_r_d["fault_class"] = "工程质量"
        antenna_r_d["fault_subclass"] = "疑似天馈接反小区"
        antenna_r_d["tuning_action"] = "【建议核查天馈】"
        antenna_r_d["root_cause"] = "【疑似天馈接反】"
        antenna_r_d["order_status"] = "01"
        max_date = max(date_list)
        antenna_r_d["order_date"] = datetime.date(int(max_date[:4]), int(max_date[4:6]), int(max_date[6:8]))
        antenna_r_d["create_time"] = datetime.datetime.now()
        antenna_r_d["rel_id2"] = None
        antenna_r_d["expert_analysis"] = 0
        antenna_r_d["field_test"] = 0
        antenna_r_d["comments"] = "-"
        antenna_r_d["province"] = "河南"
        antenna_r_d["city"] = "郑州"
        antenna_r_d["order_sorting"] = "优化"
        # antenna_r_d["antenna_feeder_type"] = None

        ##字段排序
        antenna_r_d = antenna_r_d[["order_id", "order_code", "fault_catalog", "fault_class", "fault_subclass",
                                   "cell_name", "lon", "lat", "rel_id", "tuning_action", "root_cause",
                                   "order_status",
                                   "order_date", "create_time", "rel_id2", "comments", "province", "city",
                                   "date_on_list", "expert_analysis", "field_test", "order_sorting"]]
        print("天馈接反工单生成完毕，共{0}条".format(antenna_r_d.shape[0]))
    else:
        antenna_r_d = []
        print("无天线接反小区问题")

    cur.close()
    conn.close()
    return antenna_r_d


## 天线水平波瓣异常工单
def atenna_ab_lobe(date_list,taskid, threshhold_day, trace_db_account):
    conn = psycopg2.connect(database=trace_db_account["database"], user=trace_db_account["user"],
                            password=trace_db_account["password"], host=trace_db_account["host"],
                            port=trace_db_account["port"])

    cur = conn.cursor()

    antenna_ab_data = []
    for i in range(taskid.shape[0]):
        ## 查询 7日异常小区
        temp_sql = "(select cellname,eci,celllon,celllat from public.task_" + str(taskid.loc[i, "taskid"]) + \
                   "_lte_cell_sum where azimuthcheck=1) "
        cur.execute(temp_sql)
        antenna_ab = cur.fetchall()
        if len(antenna_ab) != 0:
            antenna_ab = pd.DataFrame(antenna_ab)
            # 表描述
            des = cur.description
            # 获取表头
            colname = []
            for item in des:
                colname.append(item[0])
            antenna_ab.columns = colname
            antenna_ab["trace_date"] = taskid.loc[i, "file_filter"]

            if i == 0:
                antenna_ab_data = antenna_ab
            else:
                antenna_ab_data = pd.concat([antenna_ab_data, antenna_ab])
    if len(antenna_ab_data) != 0:
        antenna_ab_data.index = range(antenna_ab_data.shape[0])
        ##计算出镜次数
        antenna_ab_list = antenna_ab_data.groupby([antenna_ab_data["cellname"],
                                                   antenna_ab_data["eci"], antenna_ab_data["celllon"],
                                                   antenna_ab_data["celllat"]]).count()
        antenna_ab_list = antenna_ab_list.reset_index()
        antenna_ab_list = antenna_ab_list[antenna_ab_list["trace_date"] >= threshhold_day]
        antenna_ab_list["date_on_list"] = ""
        antenna_ab_list.index = range(antenna_ab_list.shape[0])

        for i in range(antenna_ab_list.shape[0]):
            for j in range(antenna_ab_data.shape[0]):
                if antenna_ab_list.loc[i, "eci"] == antenna_ab_data.loc[j, "eci"]:
                    if antenna_ab_list.loc[i, "date_on_list"] == "":
                        antenna_ab_list.loc[i, "date_on_list"] = antenna_ab_data.loc[j, "trace_date"] + "=1"
                    else:
                        antenna_ab_list.loc[i, "date_on_list"] = antenna_ab_list.loc[i, "date_on_list"] + "," + \
                                                                 antenna_ab_data.loc[j, "trace_date"] + "=1"

        for i in range(antenna_ab_list.shape[0]):
            for j in range(taskid.shape[0]):
                if taskid.loc[j, "file_filter"] not in antenna_ab_list.loc[i, "date_on_list"]:
                    antenna_ab_list.loc[i, "date_on_list"] = antenna_ab_list.loc[i, "date_on_list"] + "," + taskid.loc[
                        j, "file_filter"] + "=0"
        antenna_ab_list.rename(columns={"eci": "rel_id", "celllon": "lon", "celllat": "lat", "cellname": "cell_name"},
                               inplace=True)
        ##字段填充
        antenna_ab_list["order_id"] = antenna_ab_list.index
        antenna_ab_list["order_code"] = None
        antenna_ab_list["fault_catalog"] = "小区类"
        antenna_ab_list["fault_class"] = "工程质量"
        antenna_ab_list["fault_subclass"] = "疑似天线波瓣角异常小区"
        antenna_ab_list["tuning_action"] = "【建议核查天馈】"
        antenna_ab_list["root_cause"] = "【天线波瓣角异常】"
        antenna_ab_list["order_status"] = "01"
        max_date = max(date_list)
        antenna_ab_list["order_date"] = datetime.date(int(max_date[:4]), int(max_date[4:6]), int(max_date[6:8]))
        antenna_ab_list["create_time"] = datetime.datetime.now()
        antenna_ab_list["rel_id2"] = None
        antenna_ab_list["expert_analysis"] = 0
        antenna_ab_list["field_test"] = 0
        antenna_ab_list["comments"] = "-"
        antenna_ab_list["province"] = "河南"
        antenna_ab_list["city"] = "郑州"
        antenna_ab_list["order_sorting"] = "优化"
        # antenna_ab_list["antenna_feeder_type"] = ""
        ##字段排序
        antenna_ab_list = antenna_ab_list[["order_id", "order_code", "fault_catalog", "fault_class", "fault_subclass",
                                           "cell_name", "lon", "lat", "rel_id", "tuning_action", "root_cause",
                                           "order_status",
                                           "order_date", "create_time", "rel_id2", "comments", "province", "city",
                                           "date_on_list", "expert_analysis", "field_test", "order_sorting"]]

        print("天线水平波瓣异常工单生成完毕，共{0}条".format(antenna_ab_list.shape[0]))
    else:
        antenna_ab_list = []
        print("无天线水平波瓣异常小区问题")
    cur.close()
    conn.close()
    return antenna_ab_list


## 天线方位角错误工单
def atenna_azimuth_error(date_list,taskid, threshhold_day, trace_db_account):
    conn = psycopg2.connect(database=trace_db_account["database"], user=trace_db_account["user"],
                            password=trace_db_account["password"], host=trace_db_account["host"],
                            port=trace_db_account["port"])
    cur = conn.cursor()
    antenna_ab_data = []
    for i in range(taskid.shape[0]):
        ## 查询 7日异常小区
        temp_sql = "(select cellname,eci,celllon,celllat from public.task_" + str(taskid.loc[i, "taskid"]) + \
                   "_lte_cell_sum where azimuthcheck=2) "
        cur.execute(temp_sql)
        antenna_ab = cur.fetchall()
        if len(antenna_ab) != 0:
            antenna_ab = pd.DataFrame(antenna_ab)
            # 表描述
            des = cur.description
            # 获取表头
            colname = []
            for item in des:
                colname.append(item[0])
            antenna_ab.columns = colname
            antenna_ab["trace_date"] = taskid.loc[i, "file_filter"]

            if i == 0:
                antenna_ab_data = antenna_ab
            else:
                antenna_ab_data = pd.concat([antenna_ab_data, antenna_ab])
    if len(antenna_ab_data) != 0:
        antenna_ab_data.index = range(antenna_ab_data.shape[0])
        ##计算出镜次数
        antenna_ab_list = antenna_ab_data.groupby([antenna_ab_data["cellname"],
                                                   antenna_ab_data["eci"], antenna_ab_data["celllon"],
                                                   antenna_ab_data["celllat"]]).count()
        antenna_ab_list = antenna_ab_list.reset_index()
        antenna_ab_list = antenna_ab_list[antenna_ab_list["trace_date"] >= threshhold_day]
        antenna_ab_list["date_on_list"] = ""
        antenna_ab_list.index = range(antenna_ab_list.shape[0])

        ## 问题异常
        for i in range(antenna_ab_list.shape[0]):
            for j in range(antenna_ab_data.shape[0]):
                if antenna_ab_list.loc[i, "eci"] == antenna_ab_data.loc[j, "eci"]:
                    if antenna_ab_list.loc[i, "date_on_list"] == "":
                        antenna_ab_list.loc[i, "date_on_list"] = antenna_ab_data.loc[j, "trace_date"] + "=1"
                    else:
                        antenna_ab_list.loc[i, "date_on_list"] = antenna_ab_list.loc[i, "date_on_list"] + "," + \
                                                                 antenna_ab_data.loc[j, "trace_date"] + "=1"
        ## 填充正常日期
        for i in range(antenna_ab_list.shape[0]):
            for j in range(taskid.shape[0]):
                if taskid.loc[j, "file_filter"] not in antenna_ab_list.loc[i, "date_on_list"]:
                    antenna_ab_list.loc[i, "date_on_list"] = antenna_ab_list.loc[i, "date_on_list"] + "," + taskid.loc[
                        j, "file_filter"] + "=0"
        antenna_ab_list.rename(columns={"eci": "rel_id", "celllon": "lon", "celllat": "lat", "cellname": "cell_name"},
                               inplace=True)
        ##字段填充
        antenna_ab_list["order_id"] = antenna_ab_list.index
        antenna_ab_list["order_code"] = None
        antenna_ab_list["fault_catalog"] = "小区类"
        antenna_ab_list["fault_class"] = "工程质量"
        antenna_ab_list["fault_subclass"] = "疑似天线方向角错误小区"
        antenna_ab_list["tuning_action"] = "【建议核查天馈】"
        antenna_ab_list["root_cause"] = "【疑似天线方向角错误】"
        antenna_ab_list["order_status"] = "01"
        max_date = max(date_list)
        antenna_ab_list["order_date"] = datetime.date(int(max_date[:4]), int(max_date[4:6]), int(max_date[6:8]))
        antenna_ab_list["create_time"] = datetime.datetime.now()
        antenna_ab_list["rel_id2"] = None
        antenna_ab_list["expert_analysis"] = 0
        antenna_ab_list["field_test"] = 0
        antenna_ab_list["comments"] = "-"
        antenna_ab_list["province"] = "河南"
        antenna_ab_list["city"] = "郑州"
        antenna_ab_list["order_sorting"] = "优化"
        # antenna_ab_list["antenna_feeder_type"] = ""
        ##字段排序
        antenna_ab_list = antenna_ab_list[["order_id", "order_code", "fault_catalog", "fault_class", "fault_subclass",
                                           "cell_name", "lon", "lat", "rel_id", "tuning_action", "root_cause",
                                           "order_status",
                                           "order_date", "create_time", "rel_id2", "comments", "province", "city",
                                           "date_on_list", "expert_analysis", "field_test", "order_sorting"]]
        print("天线方位角错误工单生成完毕，共{0}条".format(antenna_ab_list.shape[0]))

    else:
        antenna_ab_list = []
        print("无天线方位角错误小区问题")

    cur.close()
    conn.close()
    return antenna_ab_list


## 小区经纬度错误工单
def cell_location_error(date_list,taskid, threshhold_day, trace_db_account):
    conn = psycopg2.connect(database=trace_db_account["database"], user=trace_db_account["user"],
                            password=trace_db_account["password"], host=trace_db_account["host"],
                            port=trace_db_account["port"])
    cur = conn.cursor()
    cell_ab_data = []
    for i in range(taskid.shape[0]):
        ## 查询 7日异常小区
        temp_sql = "(select cellname,eci,celllon,celllat from public.task_" + str(taskid.loc[i, "taskid"]) + \
                   "_lte_cell_sum where locationerr=1) "
        cur.execute(temp_sql)
        cell_ab = cur.fetchall()
        if len(cell_ab) != 0:
            cell_ab = pd.DataFrame(cell_ab)
            # 表描述
            des = cur.description
            # 获取表头
            colname = []
            for item in des:
                colname.append(item[0])
            cell_ab.columns = colname
            cell_ab["trace_date"] = taskid.loc[i, "file_filter"]

            if i == 0:
                cell_ab_data = cell_ab
            else:
                cell_ab_data = pd.concat([cell_ab_data, cell_ab])
    if len(cell_ab_data) != 0:
        cell_ab_data.index = range(cell_ab_data.shape[0])
        ##计算出镜次数
        cell_ab_list = cell_ab_data.groupby([cell_ab_data["cellname"],
                                             cell_ab_data["eci"], cell_ab_data["celllon"],
                                             cell_ab_data["celllat"]]).count()
        cell_ab_list = cell_ab_list.reset_index()
        cell_ab_list = cell_ab_list[cell_ab_list["trace_date"] >= threshhold_day]
        cell_ab_list["date_on_list"] = ""
        cell_ab_list.index = range(cell_ab_list.shape[0])

        for i in range(cell_ab_list.shape[0]):
            for j in range(cell_ab_data.shape[0]):
                if cell_ab_list.loc[i, "eci"] == cell_ab_data.loc[j, "eci"]:
                    if cell_ab_list.loc[i, "date_on_list"] == "":
                        cell_ab_list.loc[i, "date_on_list"] = cell_ab_data.loc[j, "trace_date"] + "=1"
                    else:
                        cell_ab_list.loc[i, "date_on_list"] = cell_ab_list.loc[i, "date_on_list"] + "," + \
                                                              cell_ab_data.loc[j, "trace_date"] + "=1"

        for i in range(cell_ab_list.shape[0]):
            for j in range(taskid.shape[0]):
                if taskid.loc[j, "file_filter"] not in cell_ab_list.loc[i, "date_on_list"]:
                    cell_ab_list.loc[i, "date_on_list"] = cell_ab_list.loc[i, "date_on_list"] + "," + taskid.loc[
                        j, "file_filter"] + "=0"
        cell_ab_list.rename(columns={"eci": "rel_id", "celllon": "lon", "celllat": "lat", "cellname": "cell_name"},
                            inplace=True)
        ##字段填充
        cell_ab_list["order_id"] = cell_ab_list.index
        cell_ab_list["order_code"] = None
        cell_ab_list["fault_catalog"] = "小区类"
        cell_ab_list["fault_class"] = "工程质量"
        cell_ab_list["fault_subclass"] = "疑似位置错误小区"
        cell_ab_list["tuning_action"] = "【建议核查工参信息】"
        cell_ab_list["root_cause"] = "【小区位置信息错误】"
        cell_ab_list["order_status"] = "01"
        max_date = max(date_list)
        cell_ab_list["order_date"] = datetime.date(int(max_date[:4]), int(max_date[4:6]), int(max_date[6:8]))
        cell_ab_list["create_time"] = datetime.datetime.now()
        cell_ab_list["rel_id2"] = None
        cell_ab_list["expert_analysis"] = 0
        cell_ab_list["field_test"] = 0
        cell_ab_list["comments"] = "-"
        cell_ab_list["province"] = "河南"
        cell_ab_list["city"] = "郑州"
        cell_ab_list["order_sorting"] = "优化"
        # cell_ab_list["antenna_feeder_type"] = ""
        ##字段排序
        cell_ab_list = cell_ab_list[["order_id", "order_code", "fault_catalog", "fault_class", "fault_subclass",
                                     "cell_name", "lon", "lat", "rel_id", "tuning_action", "root_cause", "order_status",
                                     "order_date", "create_time", "rel_id2", "comments", "province", "city",
                                     "date_on_list", "expert_analysis", "field_test", "order_sorting"]]
        print("小区经纬度错误工单生成完毕，共{0}条".format(cell_ab_list.shape[0]))

    else:
        cell_ab_list = []
        print("无小区经纬度错误问题")

    cur.close()
    conn.close()

    return cell_ab_list


## 室分泄露工单
def indoor_leak(date_list,taskid, threshhold_day, trace_db_account):
    conn = psycopg2.connect(database=trace_db_account["database"], user=trace_db_account["user"],
                            password=trace_db_account["password"], host=trace_db_account["host"],
                            port=trace_db_account["port"])
    cur = conn.cursor()
    cell_ab_data = []
    for i in range(taskid.shape[0]):
        ## 查询 7日异常小区
        temp_sql = "(select cellname,eci,celllon,celllat from public.task_" + str(taskid.loc[i, "taskid"]) + \
                   "_lte_cell_sum where indoorleak=1) "
        cur.execute(temp_sql)
        cell_ab = cur.fetchall()
        if len(cell_ab) != 0:
            cell_ab = pd.DataFrame(cell_ab)
            # 表描述
            des = cur.description
            # 获取表头
            colname = []
            for item in des:
                colname.append(item[0])
            cell_ab.columns = colname
            cell_ab["trace_date"] = taskid.loc[i, "file_filter"]

            if i == 0:
                cell_ab_data = cell_ab
            else:
                cell_ab_data = pd.concat([cell_ab_data, cell_ab])
    if len(cell_ab_data) != 0:
        cell_ab_data.index = range(cell_ab_data.shape[0])
        ##计算出镜次数
        cell_ab_list = cell_ab_data.groupby([cell_ab_data["cellname"],
                                             cell_ab_data["eci"], cell_ab_data["celllon"],
                                             cell_ab_data["celllat"]]).count()
        cell_ab_list = cell_ab_list.reset_index()

        cell_ab_list = cell_ab_list[cell_ab_list["trace_date"] >= threshhold_day]
        cell_ab_list["date_on_list"] = ""
        cell_ab_list.index = range(cell_ab_list.shape[0])

        for i in range(cell_ab_list.shape[0]):
            for j in range(cell_ab_data.shape[0]):
                if cell_ab_list.loc[i, "eci"] == cell_ab_data.loc[j, "eci"]:
                    if cell_ab_list.loc[i, "date_on_list"] == "":
                        cell_ab_list.loc[i, "date_on_list"] = cell_ab_data.loc[j, "trace_date"] + "=1"
                    else:
                        cell_ab_list.loc[i, "date_on_list"] = cell_ab_list.loc[i, "date_on_list"] + "," + \
                                                              cell_ab_data.loc[j, "trace_date"] + "=1"

        for i in range(cell_ab_list.shape[0]):
            for j in range(taskid.shape[0]):
                if taskid.loc[j, "file_filter"] not in cell_ab_list.loc[i, "date_on_list"]:
                    cell_ab_list.loc[i, "date_on_list"] = cell_ab_list.loc[i, "date_on_list"] + "," + taskid.loc[
                        j, "file_filter"] + "=0"

        cell_ab_list.rename(columns={"eci": "rel_id", "celllon": "lon", "celllat": "lat", "cellname": "cell_name"},
                            inplace=True)
        ##字段填充
        cell_ab_list["order_id"] = cell_ab_list.index
        cell_ab_list["order_code"] = None
        cell_ab_list["fault_catalog"] = "小区类"
        cell_ab_list["fault_class"] = "工程质量"
        cell_ab_list["fault_subclass"] = "疑似室分泄漏小区"
        cell_ab_list["tuning_action"] = "【建议调整室分小区发射功率】"
        cell_ab_list["root_cause"] = "【室分泄漏】"
        cell_ab_list["order_status"] = "01"
        max_date = max(date_list)
        cell_ab_list["order_date"] = datetime.date(int(max_date[:4]), int(max_date[4:6]), int(max_date[6:8]))
        cell_ab_list["create_time"] = datetime.datetime.now()
        cell_ab_list["rel_id2"] = None
        cell_ab_list["expert_analysis"] = 0
        cell_ab_list["field_test"] = 0
        cell_ab_list["comments"] = "-"
        cell_ab_list["province"] = "河南"
        cell_ab_list["city"] = "郑州"
        cell_ab_list["order_sorting"] = "优化"
        # cell_ab_list["antenna_feeder_type"] = ""
        ##字段排序
        cell_ab_list = cell_ab_list[["order_id", "order_code", "fault_catalog", "fault_class", "fault_subclass",
                                     "cell_name", "lon", "lat", "rel_id", "tuning_action", "root_cause", "order_status",
                                     "order_date", "create_time", "rel_id2", "comments", "province", "city",
                                     "date_on_list", "expert_analysis", "field_test", "order_sorting"]]
        print("室分泄露工单生成完毕，共{0}条".format(cell_ab_list.shape[0]))

    else:
        cell_ab_list = []
        print("无室分泄露问题")

    cur.close()
    conn.close()
    return cell_ab_list


## 越区覆盖工单
def overshooting(date_list,taskid, threshhold_day, trace_db_account):
    conn = psycopg2.connect(database=trace_db_account["database"], user=trace_db_account["user"],
                            password=trace_db_account["password"], host=trace_db_account["host"],
                            port=trace_db_account["port"])
    cur = conn.cursor()
    cell_ab_data = []
    ###SQL 查询异常小区记录
    for i in range(taskid.shape[0]):
        ## 查询 7日异常小区
        temp_sql = "(select cellname,eci,celllon,celllat from public.task_" + str(taskid.loc[i, "taskid"]) + \
                   "_lte_cell_sum where overshooting=1) "
        cur.execute(temp_sql)
        cell_ab = cur.fetchall()
        if len(cell_ab) != 0:
            cell_ab = pd.DataFrame(cell_ab)
            # 表描述
            des = cur.description
            # 获取表头
            colname = []
            for item in des:
                colname.append(item[0])
            cell_ab.columns = colname
            cell_ab["trace_date"] = taskid.loc[i, "file_filter"]

            if i == 0:
                cell_ab_data = cell_ab
            else:
                cell_ab_data = pd.concat([cell_ab_data, cell_ab])
    if len(cell_ab_data) != 0:
        cell_ab_data.index = range(cell_ab_data.shape[0])
        ##计算出镜次数
        cell_ab_list = cell_ab_data.groupby([cell_ab_data["cellname"],
                                             cell_ab_data["eci"], cell_ab_data["celllon"],
                                             cell_ab_data["celllat"]]).count()
        cell_ab_list = cell_ab_list.reset_index()
        cell_ab_list = cell_ab_list[cell_ab_list["trace_date"] >= threshhold_day]
        cell_ab_list["date_on_list"] = ""
        cell_ab_list.index = range(cell_ab_list.shape[0])

        for i in range(cell_ab_list.shape[0]):
            for j in range(cell_ab_data.shape[0]):
                if cell_ab_list.loc[i, "eci"] == cell_ab_data.loc[j, "eci"]:
                    if cell_ab_list.loc[i, "date_on_list"] == "":
                        cell_ab_list.loc[i, "date_on_list"] = cell_ab_data.loc[j, "trace_date"] + "=1"
                    else:
                        cell_ab_list.loc[i, "date_on_list"] = cell_ab_list.loc[i, "date_on_list"] + "," + \
                                                              cell_ab_data.loc[j, "trace_date"] + "=1"

        for i in range(cell_ab_list.shape[0]):
            for j in range(taskid.shape[0]):
                if taskid.loc[j, "file_filter"] not in cell_ab_list.loc[i, "date_on_list"]:
                    cell_ab_list.loc[i, "date_on_list"] = cell_ab_list.loc[i, "date_on_list"] + "," + taskid.loc[
                        j, "file_filter"] + "=0"
        cell_ab_list.rename(columns={"eci": "rel_id", "celllon": "lon", "celllat": "lat", "cellname": "cell_name"},
                            inplace=True)
        ##字段填充
        cell_ab_list["order_id"] = cell_ab_list.index
        cell_ab_list["order_code"] = None
        cell_ab_list["fault_catalog"] = "小区类"
        cell_ab_list["fault_class"] = "覆盖控制"
        cell_ab_list["fault_subclass"] = "越区覆盖小区"
        cell_ab_list["tuning_action"] = "【建议调整小区下倾角控制覆盖范围】"
        cell_ab_list["root_cause"] = "【越区覆盖】"
        cell_ab_list["order_status"] = "01"
        max_date = max(date_list)
        cell_ab_list["order_date"] = datetime.date(int(max_date[:4]), int(max_date[4:6]), int(max_date[6:8]))
        cell_ab_list["create_time"] = datetime.datetime.now()
        cell_ab_list["rel_id2"] = None
        cell_ab_list["expert_analysis"] = 0
        cell_ab_list["field_test"] = 0
        cell_ab_list["comments"] = "-"
        cell_ab_list["province"] = "河南"
        cell_ab_list["city"] = "郑州"
        cell_ab_list["order_sorting"] = "优化"
        # cell_ab_list["antenna_feeder_type"] = ""
        ##字段排序
        cell_ab_list = cell_ab_list[["order_id", "order_code", "fault_catalog", "fault_class", "fault_subclass",
                                     "cell_name", "lon", "lat", "rel_id", "tuning_action", "root_cause", "order_status",
                                     "order_date", "create_time", "rel_id2", "comments", "province", "city",
                                     "date_on_list", "expert_analysis", "field_test", "order_sorting"]]
        print("越区覆盖工单生成完毕，共{0}条".format(cell_ab_list.shape[0]))

    else:
        cell_ab_list = []
        print("无越区覆盖类问题")

    cur.close()
    conn.close()
    return cell_ab_list


## 高重建工单
def high_reest(date_list,taskid, threshhold_day, trace_db_account):
    conn = psycopg2.connect(database=trace_db_account["database"], user=trace_db_account["user"],
                            password=trace_db_account["password"], host=trace_db_account["host"],
                            port=trace_db_account["port"])
    cur = conn.cursor()
    cell_ab_data = []
    for i in range(taskid.shape[0]):
        ## 查询 7日异常小区
        temp_sql = "(select cellname,eci,celllon,celllat from public.task_" + str(taskid.loc[i, "taskid"]) + \
                   "_lte_cell_sum where conntotalnum >100 and reestreqnum*1.0/conntotalnum >0.05) "
        cur.execute(temp_sql)
        cell_ab = cur.fetchall()
        if len(cell_ab) != 0:
            cell_ab = pd.DataFrame(cell_ab)
            # 表描述
            des = cur.description
            # 获取表头
            colname = []
            for item in des:
                colname.append(item[0])
            cell_ab.columns = colname
            cell_ab["trace_date"] = taskid.loc[i, "file_filter"]

            if i == 0:
                cell_ab_data = cell_ab
            else:
                cell_ab_data = pd.concat([cell_ab_data, cell_ab])
    if len(cell_ab_data) != 0:
        cell_ab_data.index = range(cell_ab_data.shape[0])
        ##计算出镜次数
        cell_ab_list = cell_ab_data.groupby([cell_ab_data["cellname"],
                                             cell_ab_data["eci"], cell_ab_data["celllon"],
                                             cell_ab_data["celllat"]]).count()
        cell_ab_list = cell_ab_list.reset_index()
        cell_ab_list = cell_ab_list[cell_ab_list["trace_date"] >= threshhold_day]
        cell_ab_list["date_on_list"] = ""
        cell_ab_list.index = range(cell_ab_list.shape[0])

        for i in range(cell_ab_list.shape[0]):
            for j in range(cell_ab_data.shape[0]):
                if cell_ab_list.loc[i, "eci"] == cell_ab_data.loc[j, "eci"]:
                    if cell_ab_list.loc[i, "date_on_list"] == "":
                        cell_ab_list.loc[i, "date_on_list"] = cell_ab_data.loc[j, "trace_date"] + "=1"
                    else:
                        cell_ab_list.loc[i, "date_on_list"] = cell_ab_list.loc[i, "date_on_list"] + "," + \
                                                              cell_ab_data.loc[j, "trace_date"] + "=1"

        for i in range(cell_ab_list.shape[0]):
            for j in range(taskid.shape[0]):
                if taskid.loc[j, "file_filter"] not in cell_ab_list.loc[i, "date_on_list"]:
                    cell_ab_list.loc[i, "date_on_list"] = cell_ab_list.loc[i, "date_on_list"] + "," + taskid.loc[
                        j, "file_filter"] + "=0"
        cell_ab_list.rename(columns={"eci": "rel_id", "celllon": "lon", "celllat": "lat", "cellname": "cell_name"},
                            inplace=True)
        ##字段填充
        cell_ab_list["order_id"] = cell_ab_list.index
        cell_ab_list["order_code"] = None
        cell_ab_list["fault_catalog"] = "小区类"
        cell_ab_list["fault_class"] = "业务体验"
        cell_ab_list["fault_subclass"] = "高重建比例小区"
        cell_ab_list["tuning_action"] = "【建议专家分析+现场测试】"
        cell_ab_list["root_cause"] = "【1、无线环境存在干扰；2、切换失败导致】"
        cell_ab_list["order_status"] = "01"
        max_date = max(date_list)
        cell_ab_list["order_date"] = datetime.date(int(max_date[:4]), int(max_date[4:6]), int(max_date[6:8]))
        cell_ab_list["create_time"] = datetime.datetime.now()
        cell_ab_list["rel_id2"] = None
        cell_ab_list["expert_analysis"] = 0
        cell_ab_list["field_test"] = 0
        cell_ab_list["comments"] = "-"
        cell_ab_list["province"] = "河南"
        cell_ab_list["city"] = "郑州"
        cell_ab_list["order_sorting"] = "优化"
        # cell_ab_list["antenna_feeder_type"] = ""
        ##字段排序
        cell_ab_list = cell_ab_list[["order_id", "order_code", "fault_catalog", "fault_class", "fault_subclass",
                                     "cell_name", "lon", "lat", "rel_id", "tuning_action", "root_cause", "order_status",
                                     "order_date", "create_time", "rel_id2", "comments", "province", "city",
                                     "date_on_list", "expert_analysis", "field_test", "order_sorting"]]
        print("高重建小区工单生成完毕，共{0}条".format(cell_ab_list.shape[0]))

    else:
        cell_ab_list = []
        print("无重建比例高小区问题")

    cur.close()
    conn.close()
    return cell_ab_list


## 切换失败工单
def ho_fail_pairs(date_list,taskid, threshhold_day, trace_db_account):
    conn = psycopg2.connect(database=trace_db_account["database"], user=trace_db_account["user"],
                            password=trace_db_account["password"], host=trace_db_account["host"],
                            port=trace_db_account["port"])
    cur = conn.cursor()
    cell_ab_data = []
    for i in range(taskid.shape[0]):
        ## 查询异常小区
        temp_sql = "select a.serveci,a.desteci,b.cellname,b.lon,b.lat,a.hooutsuccnum,a.hoouttotalnum from " + \
                   "(select serveci,desteci,hooutsuccnum,hoouttotalnum from public.task_" + str(
            taskid.loc[i, "taskid"]) + \
                   "_lte_ho_snbcell where hoouttotalnum>=1000 and hooutsuccnum/ hoouttotalnum <0.9) a left join" + \
                   " (select cellname,lon/100000.0 as lon,lat/100000.0 as lat,cellindex as eci from public.task_" + \
                   str(taskid.loc[i, "taskid"]) + "_siteinfo)  b on a.serveci=b.eci "
        cur.execute(temp_sql)
        cell_ab = cur.fetchall()
        if len(cell_ab) != 0:

            cell_ab = pd.DataFrame(cell_ab)
            # 表描述
            des = cur.description
            # 获取表头
            colname = []
            for item in des:
                colname.append(item[0])
            cell_ab.columns = colname
            cell_ab["trace_date"] = taskid.loc[i, "file_filter"]

            if i == 0:
                cell_ab_data = cell_ab
            else:
                cell_ab_data = pd.concat([cell_ab_data, cell_ab])
    if len(cell_ab_data) != 0:
        cell_ab_data.index = range(cell_ab_data.shape[0])
        ##计算出镜次数
        cell_ab_list = cell_ab_data.groupby([cell_ab_data["cellname"], cell_ab_data["serveci"],
                                             cell_ab_data["desteci"], cell_ab_data["lon"],
                                             cell_ab_data["lat"]]).agg(
            {"trace_date": "count", "hooutsuccnum": "sum", "hoouttotalnum": "sum"}).reset_index()
        cell_ab_list["ho_success_ratio"] = cell_ab_list[["hooutsuccnum", "hoouttotalnum"]].apply(
            lambda x: x["hooutsuccnum"] * 1.0 / x["hoouttotalnum"], axis=1)

        ### 判断小区切换异常的出镜天数门限
        cell_ab_list = cell_ab_list[cell_ab_list["trace_date"] >= threshhold_day]
        cell_ab_list["date_on_list"] = ""

        if cell_ab_list.shape[0] > 100:
            ## 如果问题个数超过100个，问题清单按照出镜天数降序排列，并将前100个问题输出。
            cell_ab_list = cell_ab_list.sort_values(by=['trace_date', 'ho_success_ratio'], ascending=[False, False])
            cell_ab_list = cell_ab_list.head(100)
            cell_ab_list.index = range(cell_ab_list.shape[0])

        for i in range(cell_ab_list.shape[0]):
            for j in range(cell_ab_data.shape[0]):
                if cell_ab_list.loc[i, "serveci"] == cell_ab_data.loc[j, "serveci"] and \
                        cell_ab_list.loc[i, "desteci"] == cell_ab_data.loc[j, "desteci"]:
                    if cell_ab_list.loc[i, "date_on_list"] == "":
                        cell_ab_list.loc[i, "date_on_list"] = cell_ab_data.loc[j, "trace_date"] + "=1"
                    else:
                        cell_ab_list.loc[i, "date_on_list"] = cell_ab_list.loc[i, "date_on_list"] + "," + \
                                                              cell_ab_data.loc[j, "trace_date"] + "=1"
        for i in range(cell_ab_list.shape[0]):
            for j in range(taskid.shape[0]):
                if taskid.loc[j, "file_filter"] not in cell_ab_list.loc[i, "date_on_list"]:
                    cell_ab_list.loc[i, "date_on_list"] = cell_ab_list.loc[i, "date_on_list"] + "," + taskid.loc[
                        j, "file_filter"] + "=0"

        cell_ab_list.rename(columns={"desteci": "rel_id2", "serveci": "rel_id", "cellname": "cell_name"}, inplace=True)
        ##字段填充
        cell_ab_list["order_id"] = cell_ab_list.index
        cell_ab_list["order_code"] = None
        cell_ab_list["fault_catalog"] = "小区对"
        cell_ab_list["fault_class"] = "移动性"
        cell_ab_list["fault_subclass"] = "高切换失败小区对"
        cell_ab_list["tuning_action"] = "【建议专家分析+现场测试】"
        cell_ab_list["root_cause"] = "【1、邻区配置问题；2、存在干扰；3、参数配置问题；4、覆盖不足】"
        cell_ab_list["order_status"] = "01"
        max_date = max(date_list)
        cell_ab_list["order_date"] = datetime.date(int(max_date[:4]), int(max_date[4:6]), int(max_date[6:8]))
        cell_ab_list["create_time"] = datetime.datetime.now()
        cell_ab_list["expert_analysis"] = 0
        cell_ab_list["field_test"] = 0
        cell_ab_list["comments"] = "-"
        cell_ab_list["province"] = "河南"
        cell_ab_list["city"] = "郑州"
        cell_ab_list["order_sorting"] = "优化"
        # cell_ab_list["antenna_feeder_type"] = ""
        ##字段排序
        cell_ab_list = cell_ab_list[["order_id", "order_code", "fault_catalog", "fault_class", "fault_subclass",
                                     "cell_name", "lon", "lat", "rel_id", "tuning_action", "root_cause", "order_status",
                                     "order_date", "create_time", "rel_id2", "comments", "province", "city",
                                     "date_on_list", "expert_analysis", "field_test", "order_sorting"]]
        print("高切换失败小区对工单生成完毕，共{0}条".format(cell_ab_list.shape[0]))
    else:
        cell_ab_list = []
        print("无高切换失败类小区问题")

    cur.close()
    conn.close()
    return cell_ab_list


## 获取基站信息
def get_siteinfo(expert_db_account):
    conn = psycopg2.connect(database=expert_db_account["database"], user=expert_db_account["user"],
                            password=expert_db_account["password"], host=expert_db_account["host"],
                            port=expert_db_account["port"])
    cur = conn.cursor()

    ## 工参获取
    siteinfo_sql = """
    select
        ci as rel_id,
        antenna_type as antenna_feeder_type
    from
        cnio.siteinfo s
    where
        rattype = '4G'
        and data_date = (
        select
            max(data_date)
        from
            cnio.siteinfo s2 );
    """
    cur.execute(siteinfo_sql)
    siteinfo_data = cur.fetchall()
    if len(siteinfo_data) != 0:
        siteinfo_data = pd.DataFrame(siteinfo_data)
        siteinfo_data.columns = ["rel_id", "antenna_feeder_type"]

    print("工参获取完毕")
    return siteinfo_data


## 获取未闭环工单信息
def get_unclosed_workorder(fault_type,expert_db_account):
    conn = psycopg2.connect(database=expert_db_account["database"], user=expert_db_account["user"],
                            password=expert_db_account["password"], host=expert_db_account["host"],
                            port=expert_db_account["port"])
    cur = conn.cursor()
    ##
    unclosed_workorder_sql = """
    select
        order_code,
        rel_id ,
        cast(rel_id2 as int8),
        fault_subclass ,
        1 as status
    from
        cnio.fault_workorder fw
    where
        order_status != '04' and fault_subclass= '%s'
    """ % fault_type
    cur.execute(unclosed_workorder_sql)
    unclosed_order_data = cur.fetchall()
    if len(unclosed_order_data) != 0:
        unclosed_order_data = pd.DataFrame(unclosed_order_data)
        unclosed_order_data.columns = ["order_code","rel_id", "rel_id2", "fault_subclass", "status"]

    print("未闭环工单获取完毕")
    return unclosed_order_data

## 获取未闭环工单信息
def auto_closed_workorder(fault_type,auto_closed_list,expert_db_account):
    conn = psycopg2.connect(database=expert_db_account["database"], user=expert_db_account["user"],
                            password=expert_db_account["password"], host=expert_db_account["host"],
                            port=expert_db_account["port"])
    cur = conn.cursor()
    ##
    if len(auto_closed_list)!=0:
        for i in auto_closed_list.index:
            sql =  """
            update cnio.fault_workorder set order_status='04',auto_closed='02' where order_code=%s
            """ % auto_closed_list.loc[i,"order_code"]
            cur.execute(sql)
            conn.commit()
    else:
        sql = """
        update cnio.fault_workorder set order_status='04',auto_closed='02' where fault_subclass=%s
        """ % fault_type
        cur.execute(sql)
        conn.commit()
        print("{0}类工单全部自动闭环".format(fault_type))

def workorder_duplicate(fault_type,workorder, expert_db_account):

    # 获取未闭环工单
    unclosed_workorder_list = get_unclosed_workorder(fault_type,expert_db_account)

    if len(unclosed_workorder_list)!=0:
        # 数据类型转换
        unclosed_workorder_list["rel_id2"] = unclosed_workorder_list["rel_id2"].astype(pd.Int64Dtype())
        unclosed_workorder_list["rel_id"] = unclosed_workorder_list["rel_id"].astype(pd.Int64Dtype())
        workorder["rel_id2"] = workorder["rel_id2"].astype(pd.Int64Dtype())
        workorder["rel_id"] = workorder["rel_id"].astype(pd.Int64Dtype())
        workorder_sub_list = workorder[["rel_id", "rel_id2", "fault_subclass"]]
        workorder_sub_list["status"] = 1
        ## 工单去重
        unclosed_work_order_list_duplicate = unclosed_workorder_list[["rel_id", "rel_id2", "fault_subclass", "status"]]
        workorder = pd.merge(workorder, unclosed_work_order_list_duplicate, on=["rel_id", "rel_id2", "fault_subclass"],
                             how="left")  #
        workorder = workorder[workorder["status"] != 1]
        workorder = workorder.loc[:, ~workorder.columns.isin(["status"])]
        print("工单去重后，剩余工单{0}条".format(workorder.shape[0]))

        ## 工单自动闭环
        unclosed_workorder_list_closed=  unclosed_workorder_list[["order_code","rel_id", "rel_id2", "fault_subclass"]]
        auto_closed_workorder = pd.merge(unclosed_workorder_list_closed,workorder_sub_list,on=["rel_id", "rel_id2", "fault_subclass"],how='left')
        auto_closed_workorder = auto_closed_workorder[auto_closed_workorder["status"] != 1]
    else:
        auto_closed_workorder=[]
    return workorder,auto_closed_workorder

def workorder_topn(workorder,topn):
    if workorder.shape[0]>=topn:
        workorder = workorder.head(topn)
    return workorder

def workorder_process(date_list, threshhold_day, trace_db_account, expert_db_account,topn):
    # 获取taskid
    fault_type = {""}
    taskid = get_taskid(date_list, trace_db_account)
    print("taskid:{0}".format(taskid))
    if len(taskid) != 0:
        workorder = None
        #天线接反
        workorder_a_r = antenna_reverse(date_list,taskid, threshhold_day, trace_db_account)  ## 生成新问题小区
        if len(workorder_a_r) != 0:
            workorder_a_r,auto_closed_a_r = workorder_duplicate("疑似天馈接反小区",workorder_a_r, expert_db_account)
            workorder = pd.concat([workorder, workorder_a_r])
            if len(auto_closed_a_r)!=0:
                auto_closed_workorder("疑似天馈接反小区",auto_closed_a_r,expert_db_account)
        else:
            auto_closed_workorder("疑似天馈接反小区",workorder_a_r, expert_db_account)

        #天线水平波瓣异常
        workorder_a_a_l = atenna_ab_lobe(date_list,taskid, threshhold_day, trace_db_account)
        if len(workorder_a_a_l) != 0:
            workorder_a_a_l,auto_closed_a_a_l = workorder_duplicate("疑似天线波瓣角异常小区",workorder_a_a_l, expert_db_account)
            workorder = pd.concat([workorder, workorder_a_a_l])
            if len(auto_closed_a_a_l)!=0:
                auto_closed_workorder("疑似天线波瓣角异常小区",auto_closed_a_a_l,expert_db_account)
        else:
            auto_closed_workorder("疑似天线波瓣角异常小区",workorder_a_a_l, expert_db_account)

        #天线方向角错误
        workorder_a_a_e = atenna_azimuth_error(date_list,taskid, threshhold_day, trace_db_account)
        if len(workorder_a_a_e) != 0:
            workorder_a_a_e,auto_closed_a_a_e = workorder_duplicate("疑似天线方向角错误小区",workorder_a_a_e, expert_db_account)
            workorder = pd.concat([workorder, workorder_a_a_e])
            if len(auto_closed_a_a_e)!=0:
                auto_closed_workorder("疑似天线方向角错误小区",auto_closed_a_a_e,expert_db_account)
        else:
            auto_closed_workorder("疑似天线方向角错误小区",workorder_a_a_e, expert_db_account)

        # 小区经纬度错误
        workorder_c_l_e = cell_location_error(date_list,taskid, threshhold_day, trace_db_account)
        if len(workorder_c_l_e) != 0:
            workorder_c_l_e,auto_closed_c_l_e = workorder_duplicate("疑似位置错误小区",workorder_c_l_e, expert_db_account)
            workorder = pd.concat([workorder, workorder_c_l_e])
            if len(auto_closed_c_l_e)!=0:
                auto_closed_workorder("疑似位置错误小区", auto_closed_c_l_e,expert_db_account)
        else:
            auto_closed_workorder("疑似位置错误小区", workorder_c_l_e, expert_db_account)

        # 室分泄露
        workorder_i_l = indoor_leak(date_list,taskid, threshhold_day, trace_db_account)
        if len(workorder_i_l) != 0:
            workorder_i_l,auto_closed_i_l = workorder_duplicate("疑似室分泄漏小区",workorder_i_l, expert_db_account)
            workorder = pd.concat([workorder, workorder_i_l])
            if len(auto_closed_i_l)!=0:
                auto_closed_workorder("疑似室分泄漏小区",auto_closed_i_l, expert_db_account)
        else:
            auto_closed_workorder("疑似室分泄漏小区", workorder_i_l, expert_db_account)
        # # 越区覆盖
        workorder_o = overshooting(date_list,taskid, threshhold_day, trace_db_account)
        if len(workorder_o) != 0:
            workorder_o,auto_closed_o = workorder_duplicate("越区覆盖小区",workorder_o, expert_db_account)
            workorder_o=workorder_topn(workorder_o,topn)
            workorder = pd.concat([workorder, workorder_o])
            if len(auto_closed_o)!=0:
                auto_closed_workorder("越区覆盖小区", auto_closed_o, expert_db_account)
        else:
            auto_closed_workorder("越区覆盖小区", workorder_o, expert_db_account)

        # #  高重建小区
        workorder_h_r = high_reest(date_list,taskid, threshhold_day, trace_db_account)
        if len(workorder_h_r) != 0:
            workorder_h_r,auto_closed_h_r = workorder_duplicate("高重建比例小区",workorder_h_r, expert_db_account)
            workorder_h_r=workorder_topn(workorder_h_r,topn)
            workorder = pd.concat([workorder, workorder_h_r])
            if len(auto_closed_h_r)!=0:
                auto_closed_workorder("高重建比例小区", auto_closed_h_r, expert_db_account)
        else:
            auto_closed_workorder("高重建比例小区", workorder_h_r, expert_db_account)

        # #  高切换失败小区对
        # workorder_h_f_p = ho_fail_pairs(date_list,taskid, threshhold_day, trace_db_account)
        # if len(workorder_h_f_p) != 0:
        #     workorder_h_f_p,auto_closed_h_f_p = workorder_duplicate("高切换失败小区对",workorder_h_f_p, expert_db_account)
        #     workorder_h_f_p = workorder_topn(workorder_h_f_p, topn)
        #     workorder = pd.concat([workorder, workorder_h_f_p])
        #     if len(auto_closed_h_f_p)!=0:
        #         auto_closed_workorder("高切换失败小区对",auto_closed_h_f_p, expert_db_account)
        # else:
        #     auto_closed_workorder("高切换失败小区对", workorder_h_f_p, expert_db_account)

        ##如果工单为空，不计算
        if len(workorder) != 0:
            #  工单去重
            siteinfo_list = get_siteinfo(expert_db_account)
            if len(siteinfo_list) != 0:
                #  关联天线信息
                workorder = pd.merge(workorder, siteinfo_list, on="rel_id", how="left")
                workorder["auto_closed"]=None
                workorder["tuningaction_gnn"]=None
                workorder["model_name"]=None
            else:
                workorder["antenna_feeder_type"] = None
                workorder["auto_closed"]=None
                workorder["tuningaction_gnn"]=None
                workorder["model_name"]=None
            #  数据上传
            data_upload_stringio(workorder, "fault_workorder", expert_db_account)
            print("共计入库工单{0}条".format(workorder.shape[0]))
        else:
            print("无小区类工单生成")
    else:
        print("未成功获取taskid，无可用trace表单")


if __name__ == '__main__':
    day_num= ${delay_days}
    topn = 20
    ## 北京数据库账密
    # trace_db_account = {"database": "trace-tables", "user": "trace-tables", "password": "YJY_tra#tra502",
    #                     "host": "10.1.77.51", "port": "5432"}
    # expert_db_account = {"database": "expert-system", "user": "expert-system", "password": "YJY_exp#exp502",
    #                      "host": "10.1.77.51", "port": "5432", "options": "-c search_path=cnio"}

    ## 郑州数据库账密
    trace_db_account = {"database":"sk_data", "user":"sk","password":"CTRO4I!@#=",
                     "host":"133.160.191.108","port":"5432"}
    expert_db_account = {"database":"expert-system", "user":"expert-system","password":"YJY_exp#exp502",
                     "host":"133.160.191.111","port":"5432","options":"-c search_path=cnio"}

    date_list = date_task(day_num)
    threshhold_day = 4
    workorder_process(date_list, threshhold_day, trace_db_account, expert_db_account,topn)

