# coding=utf-8
import pandas as pd
import time
import psycopg2


def db_read(start_day,end_day,problem_type):
    ##sql参数
    sql_parameter = [start_day,end_day,problem_type]
    sql_date = [start_day,end_day ]
    # conn = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502",
    #                         host="10.1.77.51", port="5432")
    conn = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502",
                            host="133.160.191.111", port="5432")
    print("Postgres Connection success")
    cur = conn.cursor()
    # 查询问题清单
    sql_problem = """
select order_code,fault_subclass,cell_name,lon,lat,rel_id,tuning_action,root_cause,rel_id2,date_on_list,
order_sorting,order_date from cnio.fault_workorder_r2 where order_date between %s and  %s  and fault_subclass = %s
        """
    # 查询taskid
    cur.execute(sql_problem,sql_parameter)
    problem_cell = cur.fetchall()
    if len(problem_cell) == 0:
        pass
    else:
        problem_cell = pd.DataFrame(problem_cell)
        # 表描述
        des = cur.description
        # 获取表头
        colname = []
        for item in des:
            colname.append(item[0])
        problem_cell.columns = colname
    print("查询问题清单")
    # 查询pm指标
    sql_pm = """
select eci,(case when sum(dl_prb_avail_num)>0 then sum(dl_prb_use_num)/sum(dl_prb_avail_num) else -1 end) as dl_prb_use_ratio,
(case when sum(ul_prb_avail_num)>0 then sum(ul_prb_use_num)/sum(ul_prb_avail_num) else -1 end) as ul_prb_use_ratio,
avg(idletime_prb_noise_power_dbm) as ul_noise from cnio.pm_4g_workorder pgw  
where trace_date  between %s and  %s group by eci """
    # 查询taskid
    cur.execute(sql_pm, sql_date)
    cell_pm = cur.fetchall()
    if len(cell_pm) == 0:
        pass
    else:
        cell_pm = pd.DataFrame(cell_pm)
        # 表描述
        des = cur.description
        # 获取表头
        colname = []
        for item in des:
            colname.append(item[0])
        cell_pm.columns = colname
    print("查询pm指标")

    # 查询mr指标
    sql_mr = """
select eci, (case when sum(servingsample)>100 then sum(overshootingsample)*1.0/sum(servingsample) else -1 end) as overshooting_ratio,
(case when sum(totalsample)>100 then sum(overlap3sample)*1.0/sum(totalsample) else -1 end) as overlap_ratio,
(case when sum(servingsample)>100 then 1-sum(servrsrpdistr0+servrsrpdistr1+servrsrpdistr2)*1.0/sum(servingsample) else -1 end) as rsrpGe110_ratio
from cnio.trace_mr_cell tmc  where trace_date between %s and  %s  group by eci"""
    # 查询taskid
    cur.execute(sql_mr, sql_date)
    cell_mr = cur.fetchall()
    if len(cell_mr) == 0:
        pass
    else:
        cell_mr = pd.DataFrame(cell_mr)
        # 表描述
        des = cur.description
        # 获取表头
        colname = []
        for item in des:
            colname.append(item[0])
        cell_mr.columns = colname
    print("查询mr指标")

    # 查询工单是否有工程质量问题
    sql_project_problem_cell = """
select rel_id as eci,fault_subclass from cnio.fault_workorder_r2 fw  
where order_date between %s and  %s  and fault_class ='工程质量' and fault_catalog ='小区类'
"""
    # 查询taskid
    #print("test1")
    cur.execute(sql_project_problem_cell, sql_date)
    #print("test2")    
    cell_project_problem = cur.fetchall()
    if len(cell_project_problem) == 0:
        pass
    else:
        cell_project_problem = pd.DataFrame(cell_project_problem)
        # 表描述
        des = cur.description
        # 获取表头
        colname = []
        for item in des:
            colname.append(item[0])
        cell_project_problem.columns = colname
    print("查询工程质量问题")

    #查询告警数据
    sql_alarm = """
select enbid,alarm_starttime,alarm_endtime from cnio.alarm_detail dad where enbid <> '		-'  
and  (dad.vendor='华为' and (dad.alarm_content='网元连接中断' or dad.alarm_content='小区不可用告警' 
or dad.alarm_content='射频单元链路维护异常告警' or dad.alarm_content='射频单元硬件故障告警'  or dad.alarm_content='射频单元直流掉电告警'  or dad.alarm_content='射频单元业务不可用告警'  or dad.alarm_content='射频单元CPRI接口异常告警' 
or dad.alarm_content='传输光模块故障告警'  or dad.alarm_content='传输光接口异常告警'   or dad.alarm_content='RHUB光模块故障告警' 
or dad.alarm_content='RHUB光模块/电接口不在位告警'  
or dad.alarm_content='RHUB与pRRU间链路异常告警'   or dad.alarm_content='RHUB CPRI接口异常告警'  
or dad.alarm_content='BBU CPRI接口异常告警'  or dad.alarm_content='RHUB CPRI接口异常告警'  or dad.alarm_content='BBU CPRI光模块/电接口不在位告警'  
or dad.alarm_content='LTE小区退出服务(198094419)'  or dad.alarm_content='网元断链告警(198099803)'  
or dad.alarm_content='天馈驻波比异常(198098465)'  
or dad.alarm_content='RRU链路断(198097605)'  
or dad.alarm_content='设备掉电(198092295)'   or dad.alarm_content='S1断链告警(198094420)'
or dad.alarm_content='市电停电告警(198092550)') or (dad.vendor='中兴' and  dad.importance='严重') )
"""
    # 查询taskid
    cur.execute(sql_alarm)
    cell_alarm = cur.fetchall()
    print("len:{0}".format(len(cell_alarm)))
    if len(cell_alarm) == 0:
        pass
    else:
        cell_alarm = pd.DataFrame(cell_alarm)
        # 表描述
        des = cur.description
        # 获取表头
        colname = []
        for item in des:
            colname.append(item[0])
        cell_alarm.columns = colname
    print("查询告警数据")

    cur.close()
    conn.close()

    print("Postgres Connection release")

    return problem_cell,cell_pm,cell_mr,cell_project_problem,cell_alarm


##高重建小区诊断
def hrc_diagnose(work_order):
    work_order["order_sorting"]=""
    work_order.index = range(work_order.shape[0])
    for i in range(work_order.shape[0]):
        if (not pd.isnull(work_order.loc[i, "hr_and_alarm_day_num"])) and work_order.loc[
            i, "hr_and_alarm_day_num"] >= 2:
            work_order.loc[i, "order_sorting"] = "维护"
            work_order.loc[i, "root_cause"] = "基站告警"
            work_order.loc[i, "tuning_action"] = "排查告警"
        if work_order.loc[i, "order_sorting"] == "":
            if len(work_order.loc[i, "project_problem_flag"]) != 0:
                work_order.loc[i, "order_sorting"] = "维护/优化"
                work_order.loc[i, "root_cause"] = "服务小区存在工程质量问题"
                work_order.loc[i, "tuning_action"] = "排查解决服务小区工程质量问题"
        if work_order.loc[i, "order_sorting"] == "":
            if (not pd.isnull(work_order.loc[i, "ul_noise"])):
                if work_order.loc[i, "ul_noise"] >= -95:
                    work_order.loc[i, "order_sorting"] = "维护"
                    work_order.loc[i, "root_cause"] = "服务小区存在外部上行强干扰"
                    work_order.loc[i, "tuning_action"] = "排查服务小区外部上行干扰"
        if work_order.loc[i, "order_sorting"] == "":
            if (not pd.isnull(work_order.loc[i, "rsrpge110_ratio"])):
                if work_order.loc[i, "rsrpge110_ratio"] < 0.7 and work_order.loc[i, "rsrpge110_ratio"] >= 0:
                    work_order.loc[i, "order_sorting"] = "优化/建设"
                    work_order.loc[i, "root_cause"] = "服务小区为弱覆盖小区"
                    work_order.loc[i, "tuning_action"] = "优化提升服务小区覆盖质量"
        if work_order.loc[i, "order_sorting"] == "":
            if (not pd.isnull(work_order.loc[i, "overshooting_ratio"])):
                if work_order.loc[i, "overshooting_ratio"] >= 0.05:
                    work_order.loc[i, "order_sorting"] = "优化"
                    work_order.loc[i, "root_cause"] = "服务小区为越区覆盖小区"
                    work_order.loc[i, "tuning_action"] = "控制服务小区覆盖范围"
        if work_order.loc[i, "order_sorting"] == "":
            if (not pd.isnull(work_order.loc[i, "overlap_ratio"])):
                if work_order.loc[i, "overlap_ratio"] >= 0.3:
                    work_order.loc[i, "order_sorting"] = "优化"
                    work_order.loc[i, "root_cause"] = "服务小区的重叠覆盖率高"
                    work_order.loc[i, "tuning_action"] = "控制服务小区邻区覆盖范围"
        if work_order.loc[i, "order_sorting"] == "":
            if (not pd.isnull(work_order.loc[i, "dl_prb_use_ratio"])) and work_order.loc[i, "dl_prb_use_ratio"] >= 0.5:
                work_order.loc[i, "order_sorting"] = "优化/建设"
                work_order.loc[i, "root_cause"] = "服务小区高负荷"
                work_order.loc[i, "tuning_action"] = "通过负载均衡策略/覆盖范围控制/建设等方式解决"
        if work_order.loc[i, "order_sorting"] == "":
            work_order.loc[i, "order_sorting"] = "-"
            work_order.loc[i, "root_cause"] = "信息不足无法定位根因"
            work_order.loc[i, "tuning_action"] = "专家问诊+回溯信令"
    work_order = work_order.loc[:, ~work_order.columns.isin(["eci_x", "eci_y"])]

    return work_order

    ###告警判断
def alarm_correlation(reest_list, alarm):
    reest_list["hr_day_num"] = 0
    reest_list["hr_and_alarm_day_num"] = 0

    alarm["alarm_starttime"] = alarm["alarm_starttime"].apply(lambda x: x.replace("/", "-"))
    alarm["alarm_starttime_unix"] = alarm["alarm_starttime"].apply( \
        lambda x: time.mktime(time.strptime(x, '%Y-%m-%d %H:%M')))  # 转linux时间
    alarm["alarm_endtime"] = alarm["alarm_endtime"].apply(lambda x: x.replace("/", "-"))
    alarm["alarm_endtime"] = alarm["alarm_endtime"].apply(lambda x: "2022-7-5 0:0" if not len(x) else x)
    alarm["alarm_endtime_unix"] = alarm["alarm_endtime"].apply( \
        lambda x: time.mktime(time.strptime(x, '%Y-%m-%d %H:%M')))  # 转linux时间

    for i in range(reest_list.shape[0]):
        reest_list["enbid"] = reest_list["rel_id"].apply(lambda x: int(x/256))
        alarm_temp = alarm[alarm["enbid"] == reest_list.loc[i, "enbid"]]
        if alarm_temp.empty:
            continue
        else:
            ##判断当日是否有告警
            date = reest_list.loc[i, "date_on_list"]
            date_list = date.split(",")
            for day in date_list:
                if day.endswith('1'):
                    reest_list.loc[i, "hr_day_num"] = reest_list.loc[i, "hr_day_num"] + 1
                    day_list = day.split("=")
                    temp = day_list[0]
                    yy = int(temp[0:4])
                    mm = int(temp[4:6])
                    dd = int(temp[6:8])
                    date_time = time.mktime(
                        time.strptime(str(yy) + "-" + str(mm) + "-" + str(dd), "%Y-%m-%d"))  ##转unix时间 单位s
                    alarm_time = 0
                    sub_alarm = alarm_temp[((alarm_temp["alarm_starttime_unix"] - date_time) < 86400) & (
                                alarm_temp["alarm_starttime_unix"] > date_time)]
                    ###筛选出当日告警
                    sub_alarm.index = range(sub_alarm.shape[0])
                    for j in range(sub_alarm.shape[0]):
                        if sub_alarm.loc[j, "alarm_starttime_unix"] < date_time:
                            alarm_time = sub_alarm.loc[j, "alarm_endtime_unix"] - date_time + alarm_time;
                        elif sub_alarm.loc[j, "alarm_starttime_unix"] > date_time and (
                                sub_alarm.loc[j, "alarm_starttime_unix"] - date_time) < 86400:
                            alarm_time = sub_alarm.loc[j, "alarm_endtime_unix"] - sub_alarm.loc[
                                j, "alarm_starttime_unix"] + alarm_time;
                        else:
                            continue
                    # 当日告警时长＞半小时，判断为异常
                    if alarm_time > 1800:
                        reest_list.loc[i, "hr_and_alarm_day_num"] = reest_list.loc[
                                                                        i, "hr_and_alarm_day_num"] + 1
    return reest_list

def project_problem_correlation(workorder,cell_project_list):
    workorder["project_problem_flag"]=""
    cell_project_list.index= list(range(cell_project_list.shape[0]))
    for i in range(workorder.shape[0]):
        for j in range(cell_project_list.shape[0]):
            if workorder.loc[i,"rel_id"] == cell_project_list.loc[j,"eci"]:
                if workorder.loc[i,"project_problem_flag"] =="":
                    workorder.loc[i,"project_problem_flag"]=cell_project_list.loc[j,"fault_subclass"]
                else:
                    workorder.loc[i,"project_problem_flag"]=workorder.loc[i,"project_problem_flag"]+"|"+cell_project_list.loc[j,"fault_subclass"]
    return workorder
