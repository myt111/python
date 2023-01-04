import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import datetime as dt
from io import StringIO


def get_time_list(delay_hour_num):
    today = dt.datetime.now()
    print(today)
    time_max = today + dt.timedelta(hours=delay_hour_num)
    time_list = []
    for i in range(5):
        temp = time_max + dt.timedelta(hours= - i)
        temp = temp.strftime('%Y%m%d%H')
        time_list.append(int(temp))
    print(time_list)

    #time_list = [2022102112, 2022102111, 2022102110, 2022102114, 2022102113]
    print(time_list)
    return time_list


def get_problem_list(time_list, threshhod, expert_db_account):
    data = None
    conn = psycopg2.connect(database=expert_db_account["database"], user=expert_db_account["user"],
                            password=expert_db_account["password"],
                            host=expert_db_account["host"], port=expert_db_account["port"])

    cur = conn.cursor()
    # 获取不可用小区
    sql_unavail = """
        select
        cell_name as cellname,
        oid*256+cellid as eci ,
        '网络稳定性' as fault_type,
        '网络可用率低' as fault_subtype,
        min(hourtime) as start_time,
        count(*) as duration_hour,
        '00' as status,
        sum(KPI_0000000625_S)/count(*)/3600 as avg_kpi
    from
        searching.pm_4g_zdy_hour pgzh
    where
        hourtime = %s
        or hourtime = %s
        or hourtime = %s
        or hourtime = %s
        or hourtime = %s
    group by
        cell_name,
        oid,
        cellid
    having
        count(*)= 5
        and sum(case  when  (KPI_0000000625_S/3600)=%s then 1 else 0 end ) = 5;

    """
    unavaial_para = time_list.copy()
    unavaial_para.append(threshhod["unavailability"])
    cur.execute(sql_unavail, unavaial_para)
    data_unavail = cur.fetchall()
    if len(data_unavail) == 0:
        print("无可用率低问题小区")
    else:
        data_unavail = pd.DataFrame(data_unavail)
        des = cur.description
        colname = []
        for item in des:
            colname.append(item[0])
        data_unavail.columns = colname
        print("可用率低问题小区获取完毕")
        data = pd.concat([data, data_unavail])

    # 获取volte低接入小区
    sql_volte_estab = """
    select
        cell_name as cellname,
        oid * 256 + cellid as eci ,
        '业务体验' as fault_type,
        'Volte接入成功率低' as fault_subtype,
        min(hourtime) as start_time,
        count(*) as duration_hour,
        '00' as status,
        sum(KPI_0000000061 + KPI_0000000065)* 1.0 / sum(KPI_0000000051 + KPI_0000000055) as avg_kpi

    from
        searching.pm_4g_zdy_hour pgzh
    where
        hourtime = %s
        or hourtime = %s
        or hourtime = %s
        or hourtime = %s
        or hourtime = %s
    group by
        cell_name,
        oid,
        cellid
    having
        count(*)= 5
        and 
       sum(case when (KPI_0000000051 + KPI_0000000055)>100 and ((KPI_0000000061 + KPI_0000000065)* 1.0 / (KPI_0000000051 + KPI_0000000055))<%s then 1 else 0 end)= 5;
            """
    volte_estab_para = time_list.copy()
    volte_estab_para.append(threshhod["volte_estab_suc_ratio"])
    cur.execute(sql_volte_estab, volte_estab_para)
    data_volte_estab = cur.fetchall()
    if len(data_volte_estab) == 0:
        print("无volte低接入问题小区")
    else:
        data_volte_estab = pd.DataFrame(data_volte_estab)
        des = cur.description
        colname = []
        for item in des:
            colname.append(item[0])
        data_volte_estab.columns = colname
        print("volte低接入问题小区获取完毕")
        data = pd.concat([data, data_volte_estab])

    # 获取volte高掉话入小区
    sql_volte_estab = """
    select
        cell_name as cellname,
        oid * 256 + cellid as eci ,
        '业务体验' as fault_type,
        'Volte掉话率高' as fault_subtype,
        min(hourtime) as start_time,
        count(*) as duration_hour,
        '00' as status,
        sum(KPI_0000000201+KPI_0000000205)*1.0/ sum(KPI_0000000201+KPI_0000000205+KPI_0000000172+KPI_0000000176) as avg_kpi

    from
        searching.pm_4g_zdy_hour pgzh
    where
        hourtime = %s
        or hourtime = %s
        or hourtime = %s
        or hourtime = %s
        or hourtime = %s
    group by
        cell_name,
        oid,
        cellid
    having
        count(*)= 5
        and
        sum(case when (KPI_0000000201+KPI_0000000205+KPI_0000000172+KPI_0000000176)>100 and ((KPI_0000000201+KPI_0000000205)*1.0/ (KPI_0000000201+KPI_0000000205+KPI_0000000172+KPI_0000000176))>%s then 1 else 0 end)=5;
            """
    volte_drop_para = time_list.copy()
    volte_drop_para.append(threshhod["volte_drop_ratio"])
    cur.execute(sql_volte_estab, volte_drop_para)
    data_volte_drop = cur.fetchall()
    if len(data_volte_drop) == 0:
        print("无volte高掉话问题小区")
    else:
        data_volte_drop = pd.DataFrame(data_volte_drop)
        des = cur.description
        colname = []
        for item in des:
            colname.append(item[0])
        data_volte_drop.columns = colname
        print("volte高掉话问题小区获取完毕")
        data = pd.concat([data, data_volte_drop])

    # 获取数据上行速率低
    sql_data_ul_throughput = """
    select
        cell_name as cellname,
        oid * 256 + cellid as eci ,
        '业务体验' as fault_type,
        '数据上行传输速率低' as fault_subtype,
        min(hourtime) as start_time,
        count(*) as duration_hour,
        '00' as status,
        avg(KPI_0000000577) as avg_kpi

    from
        searching.pm_4g_zdy_hour pgzh
    where
        hourtime = %s
        or hourtime = %s
        or hourtime = %s
        or hourtime = %s
        or hourtime = %s
    group by
        cell_name,
        oid,
        cellid
    having
        count(*)= 5
        and
        sum(case when KPI_0000000577<%s and KPI_0000000577>0 and KPI_0000000519<10 then 1 else 0 end)=5;
            """
    data_ul_throughput_para = time_list.copy()
    data_ul_throughput_para.append(threshhod["ul_throughput"])
    cur.execute(sql_data_ul_throughput, data_ul_throughput_para)
    data_data_ul_throughput = cur.fetchall()
    if len(data_data_ul_throughput) == 0:
        print("无上行低速率问题小区")
    else:
        data_data_ul_throughput = pd.DataFrame(data_data_ul_throughput)
        des = cur.description
        colname = []
        for item in des:
            colname.append(item[0])
        data_data_ul_throughput.columns = colname
        print("上行低速率问题小区获取完毕")
        data = pd.concat([data, data_data_ul_throughput])

    # 获取数据下行速率低
    sql_data_dl_throughput = """
    select
        cell_name as cellname,
        oid * 256 + cellid as eci ,
        '业务体验' as fault_type,
        '数据下行传输速率低' as fault_subtype,
        min(hourtime) as start_time,
        count(*) as duration_hour,
        '00' as status,
        avg(KPI_0000000578) as avg_kpi

    from
        searching.pm_4g_zdy_hour pgzh
    where
        hourtime = %s
        or hourtime = %s
        or hourtime = %s
        or hourtime = %s
        or hourtime = %s
    group by
        cell_name,
        oid,
        cellid
    having
        count(*)= 5
        and
        sum(case when KPI_0000000578<%s and KPI_0000000578>0 and KPI_0000000519<10  then 1 else 0 end)=5;
            """
    data_dl_throughput_para = time_list.copy()
    data_dl_throughput_para.append(threshhod["dl_throughput"])
    cur.execute(sql_data_dl_throughput, data_dl_throughput_para)
    data_data_dl_throughput = cur.fetchall()
    if len(data_data_dl_throughput) == 0:
        print("无下行低速率问题小区")
    else:
        data_data_dl_throughput = pd.DataFrame(data_data_dl_throughput)
        des = cur.description
        colname = []
        for item in des:
            colname.append(item[0])
        data_data_dl_throughput.columns = colname
        print("下行低速率问题小区获取完毕")
        data = pd.concat([data, data_data_dl_throughput])

    # 获取数据下行速率低
    sql_dl_prb = """
    select
        cell_name as cellname,
        oid * 256 + cellid as eci ,
        '资源负荷' as fault_type,
        '下行PRB利用率高' as fault_subtype,
        min(hourtime) as start_time,
        count(*) as duration_hour,
        '00' as status,
        sum(KPI_0000000613)*1.0/sum(KPI_0000000615) as avg_kpi

    from
        searching.pm_4g_zdy_hour pgzh
    where
        hourtime = %s
        or hourtime = %s
        or hourtime = %s
        or hourtime = %s
        or hourtime = %s
    group by
        cell_name,
        oid,
        cellid
    having
        count(*)= 5
        and
        sum(case when KPI_0000000613>0 and (KPI_0000000613*1.0/KPI_0000000615) >%s then  1 else 0 end)=5;
            """
    dl_prb_para = time_list.copy()
    dl_prb_para.append(threshhod["dl_prb_utilization"])
    cur.execute(sql_dl_prb, dl_prb_para)
    data_dl_prb = cur.fetchall()
    if len(data_dl_prb) == 0:
        print("无下行PRB利用率高问题小区")
    else:
        data_dl_prb = pd.DataFrame(data_dl_prb)
        des = cur.description
        colname = []
        for item in des:
            colname.append(item[0])
        data_dl_prb.columns = colname
        print("下行PRB利用率高问题小区获取完毕")
        data = pd.concat([data, data_dl_prb])

    # 获取上行prb利用率高
    sql_ul_prb = """
    select
        cell_name as cellname,
        oid * 256 + cellid as eci ,
        '资源负荷' as fault_type,
        '上行PRB利用率高' as fault_subtype,
        min(hourtime) as start_time,
        count(*) as duration_hour,
        '00' as status,
        sum(KPI_0000000612)*1.0/sum(KPI_0000000614) as avg_kpi

    from
        searching.pm_4g_zdy_hour pgzh
    where
        hourtime = %s
        or hourtime = %s
        or hourtime = %s
        or hourtime = %s
        or hourtime = %s
    group by
        cell_name,
        oid,
        cellid
    having
        count(*)= 5
        and
        sum(case when KPI_0000000614>0 and (KPI_0000000612*1.0/KPI_0000000614) >%s then  1 else 0 end)=5;
            """
    ul_prb_para = time_list.copy()
    ul_prb_para.append(threshhod["ul_prb_utilization"])
    cur.execute(sql_ul_prb, ul_prb_para)
    data_ul_prb = cur.fetchall()
    data_ul_prb = pd.DataFrame(data_ul_prb)

    if len(data_ul_prb) == 0:
        print("无上行PRB利用率高问题小区")
    else:
        des = cur.description
        colname = []
        for item in des:
            colname.append(item[0])
        data_ul_prb.columns = colname
        print("上行PRB利用率高问题小区获取完毕")
        data = pd.concat([data, data_ul_prb])

    # 获取系统内切出成功率低小区
    sql_l2l_hout = """
    select
        cell_name as cellname,
        oid * 256 + cellid as eci ,
        '关键性能' as fault_type,
        '系统内切换成功率低' as fault_subtype,
        min(hourtime) as start_time,
        count(*) as duration_hour,
        '00' as status,
        sum(KPI_0000000298+KPI_0000000300)*1.0/sum(KPI_0000000297+KPI_0000000299) as avg_kpi

    from
        searching.pm_4g_zdy_hour pgzh
    where
        hourtime = %s
        or hourtime = %s
        or hourtime = %s
        or hourtime = %s
        or hourtime = %s
    group by
        cell_name,
        oid,
        cellid
    having
        count(*)= 5
        and
        sum(case when (KPI_0000000297+KPI_0000000299)>100 and ((KPI_0000000298+KPI_0000000300)*1.0/(KPI_0000000297+KPI_0000000299)) <%s
        then  1 else 0 end)=5;
            """
    l2l_hoout_para = time_list.copy()
    l2l_hoout_para.append(threshhod["l2l_hoout_suc_ratio"])
    cur.execute(sql_l2l_hout, l2l_hoout_para)
    data_l2l_hoout = cur.fetchall()
    if len(data_l2l_hoout) == 0:
        print("无系统内切换失败问题小区")
    else:
        data_l2l_hoout = pd.DataFrame(data_l2l_hoout)
        des = cur.description
        colname = []
        for item in des:
            colname.append(item[0])
        data_l2l_hoout.columns = colname
        print("系统内切换失败问题小区获取完毕")
        data = pd.concat([data, data_l2l_hoout])

    # 获取4切3切出成功率低小区
    sql_l2w_hout = """
    select
        cell_name as cellname,
        oid * 256 + cellid as eci ,
        '关键性能' as fault_type,
        '4G切3G成功率低' as fault_subtype,
        min(hourtime) as start_time,
        count(*) as duration_hour,
        '00' as status,
        sum(KPI_0000000306)*1.0/sum(KPI_0000000304) as avg_kpi

    from
        searching.pm_4g_zdy_hour pgzh
    where
        hourtime = %s
        or hourtime = %s
        or hourtime = %s
        or hourtime = %s
        or hourtime = %s
    group by
        cell_name,
        oid,
        cellid
    having
        count(*)= 5
        and
        sum(case when KPI_0000000304>100 and (KPI_0000000306*1.0/KPI_0000000304) <%s
        then  1 else 0 end)=5;
            """
    l2w_hoout_para = time_list.copy()
    l2w_hoout_para.append(threshhod["l2w_hout_suc_ratio"])
    cur.execute(sql_l2l_hout, l2w_hoout_para)
    data_l2w_hoout = cur.fetchall()
    if len(data_l2w_hoout) == 0:
        print("无4切3高切换失败率问题小区")
    else:
        data_l2w_hoout = pd.DataFrame(data_l2w_hoout)
        des = cur.description
        colname = []
        for item in des:
            colname.append(item[0])
        data_l2w_hoout.columns = colname
        print("4切3高切换失败率问题小区获取完毕")
        data = pd.concat([data, data_l2w_hoout])

    cur.close()
    conn.close()
    return data


def get_unclosed_order_list(expert_db_account):
    conn = psycopg2.connect(database=expert_db_account["database"], user=expert_db_account["user"],
                            password=expert_db_account["password"],
                            host=expert_db_account["host"], port=expert_db_account["port"])

    cur = conn.cursor()
    sql_order = """
    select
        order_id,
        eci,
        fault_type ,
        fault_subtype,
        1 as flag
    from
        cnio.lte_kpi_monitor s
    where
        status = '00'
    """
    cur.execute(sql_order)
    unclosed = cur.fetchall()
    if len(unclosed) == 0:
        print("无未闭关工单")
    else:
        unclosed = pd.DataFrame(unclosed)
        des = cur.description
        colname = []
        for item in des:
            colname.append(item[0])
        unclosed.columns = colname
        print("未闭环工单读取完毕")
    cur.close()
    conn.close()
    return unclosed


def order_duplicate_and_close(problem_list, expert_db_account):
    problem_list_sub = problem_list[["eci", "fault_type", "fault_subtype"]]
    problem_list_sub["problem_flag"] = 1

    #
    unclosed = get_unclosed_order_list(expert_db_account)

    if len(unclosed) != 0:
        ## 去重后新工单
        unclosed_sub = unclosed[["eci", "fault_type", "fault_subtype", "order_id", "flag"]]
        unclosed_sub.rename(columns={"order_id": "order_code"}, inplace=True)
        problem_list = pd.merge(problem_list, unclosed_sub, on=["eci", "fault_type", "fault_subtype"], how='left')
        problem_list_new = problem_list[problem_list["flag"] != 1]
        problem_list_new = problem_list_new.loc[:, ~problem_list_new.columns.isin(["flag", "order_code"])]
        ## 旧工单 需更新工单持续时长
        unclosed_list_old = problem_list[problem_list["flag"] == 1]

        ## 闭环
        closed = pd.merge(unclosed, problem_list_sub, on=["eci", "fault_type", "fault_subtype"], how='left')
        closed_list = closed[closed["problem_flag"] != 1]
    else:
        closed_list = []
        unclosed_list_old = []
        problem_list_new = problem_list
    return problem_list_new, closed_list, unclosed_list_old


def update_closed_order(closed_list, expert_db_account):
    conn = psycopg2.connect(database=expert_db_account["database"], user=expert_db_account["user"],
                            password=expert_db_account["password"],
                            host=expert_db_account["host"], port=expert_db_account["port"])

    cur = conn.cursor()
    for i in closed_list.index:
        sql = """
        update cnio.lte_kpi_monitor set status='01',close_type='00' where order_id='%s'  
        """ % closed_list.loc[i, "order_id"]
        cur.execute(sql)
        conn.commit()
    print("闭环工单更新完毕")
    cur.close()
    conn.close()


def update_unclosed_order(unclosed, expert_db_account):
    conn = psycopg2.connect(database=expert_db_account["database"], user=expert_db_account["user"],
                            password=expert_db_account["password"],
                            host=expert_db_account["host"], port=expert_db_account["port"])

    cur = conn.cursor()
    for i in unclosed.index:
        sql = """
        update cnio.lte_kpi_monitor set duration_hour=duration_hour+1 where order_id='%s'  
        """ % unclosed.loc[i, "order_code"]
        cur.execute(sql)
        conn.commit()
    print("未闭环工单更新完毕")
    cur.close()
    conn.close()


def data_format_modify(data):
    fault_type_map = {"网络稳定性": "1", "资源负荷": "2", "业务体验": "3", "关键性能": "4"}
    fault_subtype_map = {"网络可用率低": "01", "上行PRB利用率高": "02", "下行PRB利用率高": "03", "Volte接入成功率低": "04",
                         "Volte掉话率高": "05", "数据上行传输速率低": "06", "数据下行传输速率低": "07",
                         "系统内切换成功率低": "08", "4G切3G成功率低": "09"}
    data.index = range(data.shape[0])

    for i in data.index:
        if i == 1462:
            print("ok")
        temp= str(data.loc[i, "start_time"]) + fault_type_map[data.loc[i, "fault_type"]] + fault_subtype_map[
                data.loc[i, "fault_subtype"]] + str(i)
        data.loc[i, "order_id"]=temp
    ## 时间换换成时间戳格式
    data["start_time"] = data["start_time"].apply(
        lambda x: (dt.datetime.strptime(str(x), '%Y%m%d%H')).strftime('%Y-%m-%d %H%M%S'))
    data["end_time"] = None
    data["close_type"] = None
    data["cause"] = None
    data["action"] = None
    data["create_time"] = dt.datetime.today()

    data_mod = data[["order_id", "eci", "cellname", "fault_type", "fault_subtype",
                     "start_time", "end_time", "duration_hour", "status", "close_type",
                     "cause", "action", "create_time", "avg_kpi"]]
    return data_mod


def data_upload(data, expert_db_account):
    database = expert_db_account["database"]
    user = expert_db_account["user"]
    password = expert_db_account["password"]
    host = expert_db_account["host"]
    port = expert_db_account["port"]
    engine_str = 'postgresql://' + user + ":" + password + "@" + host + ":" + port + "/" + database
    engine = create_engine(engine_str)
    data.to_sql("lte_kpi_monitor", engine, schema="cnio", index=False, if_exists='append', method='multi')
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


if __name__ == '__main__':
    hour_num = ${delay_hours}
    time_list = get_time_list(hour_num)
    threshhod = {"unavailability": 1, "volte_estab_suc_ratio": 0.97, "volte_drop_ratio": 0.03,
                 "ul_throughput": 0.1, "dl_throughput": 0.5, "ul_prb_utilization": 0.8,
                 "dl_prb_utilization": 0.8, "l2l_hoout_suc_ratio": 0.7, 'l2w_hout_suc_ratio': 0.7,
                 'l2n_hout_suc_ratio': 0.7}

    # 郑州数据库连接
    # trace_db_account = {"database": "sk_data", "user": "sk", "password": "CTRO4I!@#=",
    #                     "host": "133.160.191.108", "port": "5432"}
    expert_db_account = {"database": "expert-system", "user": "expert-system", "password": "YJY_exp#exp502",
                          "host": "133.160.191.111", "port": "5432", "options": "-c search_path=cnio"}
    # 北京数据库连接
    # trace_db_account = {"database": "trace-tables", "user": "trace-tables", "password": "YJY_tra#tra502",
    #                     "host": "10.1.77.51", "port": "5432"}
    #expert_db_account = {"database": "expert-system", "user": "expert-system", "password": "YJY_exp#exp502",
    #                     "host": "10.1.77.51", "port": "5432", "options": "-c search_path=cnio"}

    data = get_problem_list(time_list, threshhod, expert_db_account)

    if data is not None:
        data_format = data_format_modify(data)
        data_problem, closed, unclosed = order_duplicate_and_close(data_format, expert_db_account)
        if len(data_problem) != 0:
            data_upload(data_format, expert_db_account)
        if len(closed) != 0:
            update_closed_order(closed, expert_db_account)
        if len(unclosed) != 0:
            update_unclosed_order(unclosed, expert_db_account)
    else:
        print("未生成问题小区，无数据同步")
