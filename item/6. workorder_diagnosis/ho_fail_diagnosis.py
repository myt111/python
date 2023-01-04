import pandas as pd
import time
import psycopg2
import common_func as cf


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

    # 查询工单是否有工程质量问题
    sql_project_problem_cell = """
select rel_id as eci,fault_subclass from cnio.fault_workorder_r2 fw  
where order_date between %s and  %s  and fault_class ='工程质量' and fault_catalog ='小区类'
"""
    # 查询taskid
    cur.execute(sql_project_problem_cell, sql_date)
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

    #查询告警数据
    sql_parameter_date = [start_day,end_day,start_day,end_day]
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

    # 工参查询
    sql_siteinfo= """
select rattype,channel,pci,siteid,cellid,azimuth,hbwd,indoor,height ,etilt,mtilt,lon,lat,
band,contractor,vendor  from cnio.siteinfo s where data_date = (select max(data_date) from cnio.siteinfo )
"""
    # 查询taskid
    cur.execute(sql_siteinfo)
    siteinfo = cur.fetchall()
    if len(siteinfo) == 0:
        pass
    else:
        siteinfo = pd.DataFrame(siteinfo)
        # 表描述
        des = cur.description
        # 获取表头
        colname = []
        for item in des:
            colname.append(item[0])
        siteinfo.columns = colname

    cur.close()
    conn.close()

    print("Postgres Connection release")

    return problem_cell,cell_pm,cell_mr,cell_project_problem,cell_alarm,siteinfo

def ho_diagnosis(ho_fail_list):
    ho_fail_list["order_sorting"] = ""
    ho_fail_list["root_cause"] = ""
    ho_fail_list["tuning_action"] = ""
    ho_fail_list.index = range(ho_fail_list.shape[0])
    ho_fail_list["ul_noise"].fillna(-120,inplace=True)
    ho_fail_list[["ul_noise", "n_ul_noise"]] = ho_fail_list[["ul_noise", "n_ul_noise"]].fillna(-120)
    ho_fail_list.fillna(-1,inplace=True)


    for i in range(ho_fail_list.shape[0]):
        if ((not pd.isnull(ho_fail_list.loc[i, "ho_fail_and_alarm_day_num"])) and ho_fail_list.loc[
            i, "ho_fail_and_alarm_day_num"] >= 2) or ((not pd.isnull(ho_fail_list.loc[i, "n_ho_fail_and_alarm_day_num"])) and ho_fail_list.loc[
            i, "n_ho_fail_and_alarm_day_num"] >= 2):
            ho_fail_list.loc[i, "order_sorting"] = "维护"
            ho_fail_list.loc[i, "root_cause"] = "基站告警"
            ho_fail_list.loc[i, "tuning_action"] = "排查告警"
        # if ho_fail_list.loc[i,"order_sorting"]=="":
        #     if  pd.isnull(ho_fail_list.loc[i,"neighbour_flag"]) :
        #         ho_fail_list.loc[i,"order_sorting"] = "优化"
        #         ho_fail_list.loc[i,"root_cause"] = "邻区漏配"
        #         ho_fail_list.loc[i,"tuning_action"] = "配置源小区到目标小区的邻区关系"
        # if ho_fail_list.loc[i, "order_sorting"] == "":
        #     if not pd.isnull(ho_fail_list.loc[i, "pci_confuse_eci"]):
        #         ho_fail_list.loc[i, "order_sorting"] = "优化"
        #         ho_fail_list.loc[i, "root_cause"] = "目标小区pci混淆"
        #         ho_fail_list.loc[i, "tuning_action"] = "重配置目标小区及混淆邻区的pci"
        if ho_fail_list.loc[i, "order_sorting"] == "":
            if len(ho_fail_list.loc[i, "src_project_problem_flag"]) != 0 and len(
                    ho_fail_list.loc[i, "tar_project_problem_flag"]) != 0:
                ho_fail_list.loc[i, "order_sorting"] = "维护/优化"
                ho_fail_list.loc[i, "root_cause"] = "源小区及目标小区存在工程质量问题"
                ho_fail_list.loc[i, "tuning_action"] = "排查解决源小区及目标小区工程质量问题"
            elif len(ho_fail_list.loc[i, "src_project_problem_flag"]) != 0 and len(
                    ho_fail_list.loc[i, "tar_project_problem_flag"]) == 0:
                ho_fail_list.loc[i, "order_sorting"] = "维护/优化"
                ho_fail_list.loc[i, "root_cause"] = "源小区及存在工程质量问题"
                ho_fail_list.loc[i, "tuning_action"] = "排查解决源小区工程质量问题"
            elif len(ho_fail_list.loc[i, "src_project_problem_flag"]) == 0 and len(
                    ho_fail_list.loc[i, "tar_project_problem_flag"]) != 0:
                ho_fail_list.loc[i, "order_sorting"] = "维护/优化"
                ho_fail_list.loc[i, "root_cause"] = "目标小区及存在工程质量问题"
                ho_fail_list.loc[i, "tuning_action"] = "排查解决目标小区工程质量问题"
        if ho_fail_list.loc[i, "order_sorting"] == "":
            if (not pd.isnull(ho_fail_list.loc[i, "ul_noise"])) and (
            not pd.isnull(ho_fail_list.loc[i, "n_ul_noise"])):
                if ho_fail_list.loc[i, "ul_noise"] >= -95 and ho_fail_list.loc[i, "n_ul_noise"] >= -95:
                    ho_fail_list.loc[i, "order_sorting"] = "维护"
                    ho_fail_list.loc[i, "root_cause"] = "源小区及目标小区存在外部上行强干扰"
                    ho_fail_list.loc[i, "tuning_action"] = "排查源小区及目标小区外部上行干扰"
                elif ho_fail_list.loc[i, "ul_noise"] >= -95 and ho_fail_list.loc[i, "n_ul_noise"] < -95:
                    ho_fail_list.loc[i, "order_sorting"] = "维护"
                    ho_fail_list.loc[i, "root_cause"] = "源小区存在外部上行强干扰"
                    ho_fail_list.loc[i, "tuning_action"] = "排查源小区外部上行干扰"
                elif ho_fail_list.loc[i, "ul_noise"] < -95 and ho_fail_list.loc[i, "n_ul_noise"] >= -95:
                    ho_fail_list.loc[i, "order_sorting"] = "维护"
                    ho_fail_list.loc[i, "root_cause"] = "目标小区存在外部上行强干扰"
                    ho_fail_list.loc[i, "tuning_action"] = "排查目标小区外部上行干扰"
            elif (not pd.isnull(ho_fail_list.loc[i, "ul_noise"])) and (
            pd.isnull(ho_fail_list.loc[i, "n_ul_noise"])):
                if ho_fail_list.loc[i, "ul_noise"] >= -95:
                    ho_fail_list.loc[i, "order_sorting"] = "维护"
                    ho_fail_list.loc[i, "root_cause"] = "源小区存在外部上行强干扰"
                    ho_fail_list.loc[i, "tuning_action"] = "排查源小区外部上行干扰"
            elif (pd.isnull(ho_fail_list.loc[i, "ul_noise"])) and (
            not pd.isnull(ho_fail_list.loc[i, "n_ul_noise"])):
                if ho_fail_list.loc[i, "n_ul_noise"] >= -95:
                    ho_fail_list.loc[i, "order_sorting"] = "维护"
                    ho_fail_list.loc[i, "root_cause"] = "目标小区存在外部上行强干扰"
                    ho_fail_list.loc[i, "tuning_action"] = "排查目标小区外部上行干扰"
        if ho_fail_list.loc[i, "order_sorting"] == "":
            if (not pd.isnull(ho_fail_list.loc[i, "rsrpge110_ratio"])) and (
            not pd.isnull(ho_fail_list.loc[i, "n_rsrpge110_ratio"])):
                if ho_fail_list.loc[i, "n_rsrpge110_ratio"] < 0.7 and ho_fail_list.loc[
                    i, "n_rsrpge110_ratio"] < 0.7:
                    ho_fail_list.loc[i, "order_sorting"] = "优化/建设"
                    ho_fail_list.loc[i, "root_cause"] = "源小区及目标小区为弱覆盖小区"
                    ho_fail_list.loc[i, "tuning_action"] = "优化提升源小区及目标小区覆盖质量"
                elif ho_fail_list.loc[i, "rsrpge110_ratio"] < 0.7 and ho_fail_list.loc[
                    i, "n_rsrpge110_ratio"] >= 0.7:
                    ho_fail_list.loc[i, "order_sorting"] = "优化/建设"
                    ho_fail_list.loc[i, "root_cause"] = "源小区为弱覆盖小区"
                    ho_fail_list.loc[i, "tuning_action"] = "优化提升源小区覆盖质量"
                elif ho_fail_list.loc[i, "rsrpge110_ratio"] >= 0.7 and ho_fail_list.loc[
                    i, "n_rsrpge110_ratio"] < 0.7:
                    ho_fail_list.loc[i, "order_sorting"] = "优化/建设"
                    ho_fail_list.loc[i, "root_cause"] = "目标小区为弱覆盖小区"
                    ho_fail_list.loc[i, "tuning_action"] = "优化提升目标小区覆盖质量"
            elif (not pd.isnull(ho_fail_list.loc[i, "rsrpge110_ratio"])) and (
            pd.isnull(ho_fail_list.loc[i, "n_rsrpge110_ratio"])):
                if ho_fail_list.loc[i, "rsrpge110_ratio"] < 0.7:
                    ho_fail_list.loc[i, "order_sorting"] = "优化/建设"
                    ho_fail_list.loc[i, "root_cause"] = "源小区为弱覆盖小区"
                    ho_fail_list.loc[i, "tuning_action"] = "优化提升源小区覆盖质量"
            elif (pd.isnull(ho_fail_list.loc[i, "rsrpge110_ratio"])) and (
            not pd.isnull(ho_fail_list.loc[i, "n_rsrpge110_ratio"])):
                if ho_fail_list.loc[i, "n_rsrpge110_ratio"] < 0.7:
                    ho_fail_list.loc[i, "order_sorting"] = "优化/建设"
                    ho_fail_list.loc[i, "root_cause"] = "目标小区为弱覆盖小区"
                    ho_fail_list.loc[i, "tuning_action"] = "优化提升目标小区覆盖质量"
        if ho_fail_list.loc[i, "order_sorting"] == "":
            if (not pd.isnull(ho_fail_list.loc[i, "overshooting_ratio"])) and (
            not pd.isnull(ho_fail_list.loc[i, "n_overshooting_ratio"])):
                if ho_fail_list.loc[i, "overshooting_ratio"] >= 0.05 and ho_fail_list.loc[
                    i, "n_overshooting_ratio"] >= 0.05:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                    ho_fail_list.loc[i, "root_cause"] = "源小区及目标小区为越区覆盖小区"
                    ho_fail_list.loc[i, "tuning_action"] = "控制源小区及目标小区覆盖范围"
                elif ho_fail_list.loc[i, "overshooting_ratio"] >= 0.05 and ho_fail_list.loc[
                    i, "n_overshooting_ratio"] < 0.05:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                    ho_fail_list.loc[i, "root_cause"] = "源小区为越区覆盖小区"
                    ho_fail_list.loc[i, "tuning_action"] = "控制源小区覆盖范围"
                elif ho_fail_list.loc[i, "overshooting_ratio"] < 0.05 and ho_fail_list.loc[
                    i, "n_overshooting_ratio"] >= 0.05:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                    ho_fail_list.loc[i, "root_cause"] = "目标小区为越区覆盖小区"
                    ho_fail_list.loc[i, "tuning_action"] = "控制目标小区覆盖范围"
            elif (not pd.isnull(ho_fail_list.loc[i, "overshooting_ratio"])) and (
            pd.isnull(ho_fail_list.loc[i, "n_overshooting_ratio"])):
                if ho_fail_list.loc[i, "overshooting_ratio"] >= 0.05:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                    ho_fail_list.loc[i, "root_cause"] = "源小区为越区覆盖小区"
                    ho_fail_list.loc[i, "tuning_action"] = "控制源小区覆盖范围"
            elif (pd.isnull(ho_fail_list.loc[i, "overshooting_ratio"])) and (
            not pd.isnull(ho_fail_list.loc[i, "n_overshooting_ratio"])):
                if ho_fail_list.loc[i, "n_overshooting_ratio"] >= 0.05:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                    ho_fail_list.loc[i, "root_cause"] = "目标小区为越区覆盖小区"
                    ho_fail_list.loc[i, "tuning_action"] = "控制目标小区覆盖范围"
        if ho_fail_list.loc[i, "order_sorting"] == "":
            if (not pd.isnull(ho_fail_list.loc[i, "overlap_ratio"])) and (
            not pd.isnull(ho_fail_list.loc[i, "n_overlap_ratio"])):
                if ho_fail_list.loc[i, "overlap_ratio"] >= 0.3 and ho_fail_list.loc[i, "n_overlap_ratio"] >= 0.3:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                    ho_fail_list.loc[i, "root_cause"] = "源小区及目标小区的重叠覆盖率高"
                    ho_fail_list.loc[i, "tuning_action"] = "控制源小区及目标小区邻区覆盖范围"
                elif ho_fail_list.loc[i, "overlap_ratio"] >= 0.3 and ho_fail_list.loc[i, "n_overlap_ratio"] < 0.3:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                    ho_fail_list.loc[i, "root_cause"] = "源小区的重叠覆盖率高"
                    ho_fail_list.loc[i, "tuning_action"] = "控制源小区邻区覆盖范围"
                elif ho_fail_list.loc[i, "overlap_ratio"] < 0.3 and ho_fail_list.loc[i, "n_overlap_ratio"] >= 0.3:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                    ho_fail_list.loc[i, "root_cause"] = "目标小区的重叠覆盖率高"
                    ho_fail_list.loc[i, "tuning_action"] = "控制目标小区邻区覆盖范围"
            elif (not pd.isnull(ho_fail_list.loc[i, "overlap_ratio"])) and (
            pd.isnull(ho_fail_list.loc[i, "n_overlap_ratio"])):
                if ho_fail_list.loc[i, "overlap_ratio"] >= 0.3:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                    ho_fail_list.loc[i, "root_cause"] = "源小区的重叠覆盖率高"
                    ho_fail_list.loc[i, "tuning_action"] = "控制源小区邻区覆盖范围"
            elif (pd.isnull(ho_fail_list.loc[i, "overlap_ratio"])) and (
            not pd.isnull(ho_fail_list.loc[i, "n_overlap_ratio"])):
                if ho_fail_list.loc[i, "n_overlap_ratio"] >= 0.3:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                    ho_fail_list.loc[i, "root_cause"] = "目标小区的重叠覆盖率高"
                    ho_fail_list.loc[i, "tuning_action"] = "控制目标小区附近邻区覆盖范围"
        if ho_fail_list.loc[i, "order_sorting"] == "":
            if (not pd.isnull(ho_fail_list.loc[i, "n_dl_prb_use_ratio"])) and ho_fail_list.loc[
                i, "n_dl_prb_use_ratio"] >= 0.5:
                ho_fail_list.loc[i, "order_sorting"] = "优化/建设"
                ho_fail_list.loc[i, "root_cause"] = "目标小区高负荷"
                ho_fail_list.loc[i, "tuning_action"] = "通过负载均衡策略/覆盖范围控制/建设等方式解决"
        if ho_fail_list.loc[i, "order_sorting"] == "":
            ho_fail_list.loc[i, "order_sorting"] = "-"
            ho_fail_list.loc[i, "root_cause"] = "信息不足无法判断根因"
            ho_fail_list.loc[i, "tuning_action"] = "专家问诊+回溯信令"
    ho_fail_list = ho_fail_list.loc[:, ~ho_fail_list.columns.isin(["eci_x", "eci_y", "n_eci_x", "n_eci_y"])]


    return ho_fail_list



def project_problem_correlation(ho_fail_list,cell_project_list):
    ho_fail_list["src_project_problem_flag"]=""
    ho_fail_list["tar_project_problem_flag"] = ""
    cell_project_list.index= list(range(cell_project_list.shape[0]))
    for i in range(ho_fail_list.shape[0]):
        for j in range(cell_project_list.shape[0]):
            if ho_fail_list.loc[i,"rel_id"] == cell_project_list.loc[j,"eci"]:
                if ho_fail_list.loc[i,"src_project_problem_flag"] =="":
                    ho_fail_list.loc[i,"src_project_problem_flag"]=cell_project_list.loc[j,"fault_subclass"]
                else:
                    ho_fail_list.loc[i,"src_project_problem_flag"]=ho_fail_list.loc[i,"src_project_problem_flag"]+"|"+cell_project_list.loc[j,"fault_subclass"]
            elif ho_fail_list.loc[i,"rel_id2"] == cell_project_list.loc[j,"eci"]:
                if ho_fail_list.loc[i,"tar_project_problem_flag"] =="":
                    ho_fail_list.loc[i,"tar_project_problem_flag"]=cell_project_list.loc[j,"fault_subclass"]
                else:
                    ho_fail_list.loc[i,"tar_project_problem_flag"]=ho_fail_list.loc[i,"tar_project_problem_flag"]+"|"+cell_project_list.loc[j,"fault_subclass"]

    return ho_fail_list


def pm_correlation(ho_fail_list,cell_pm):
    hfl_add_pm = pd.merge(ho_fail_list,cell_pm,left_on=["rel_id"],right_on=["eci"],how='left')
    cell_pm.rename(columns = {"eci":"n_eci","dl_prb_use_ratio":"n_dl_prb_use_ratio","ul_noise":"n_ul_noise"},inplace=True)
    hfl_add_pm = pd.merge(hfl_add_pm,cell_pm,left_on=["rel_id2"],right_on=["n_eci"],how='left')

    return hfl_add_pm

def mr_correlation(ho_fail_list,cell_mr):
    hfl_add_mr = pd.merge(ho_fail_list,cell_mr,left_on=["rel_id"],right_on=["eci"],how='left')
    cell_mr.rename(columns = {"overshooting_ratio":"n_overshooting_ratio","overlap_ratio":"n_overlap_ratio",
                              "rsrpge110_ratio":"n_rsrpge110_ratio"},inplace=True)
    hfl_add_mr = pd.merge(hfl_add_mr,cell_mr,left_on=["rel_id2"],right_on=["eci"],how='left')
    return hfl_add_mr




##切换类型判断
def ho_type(ho_fail_list,siteinfo):
    ##切换两小区间距离
    ho_fail_list["dis_srcell_to_tarcell"] = -1

    ##是否异系统切换
    ho_fail_list["irat_flag"] = -1
    ##是否异频切换
    ho_fail_list["intrafrequency_flag"] = -1
    ##是否室内外切换
    ho_fail_list["indoor_outdoor_ho_type"] = -1
    site = siteinfo[["rattype","channel","pci","siteid","cellid","azimuth","hbwd","indoor","height","etilt","mtilt","band","contractor","vendor"]]
    nsite = siteinfo[["rattype","channel","pci","siteid","cellid","azimuth","hbwd","indoor","height","etilt","mtilt","lon","lat","band","contractor","vendor"]]
    nsite.rename(
        columns={'rattype': 'n_rattype', 'channel': 'n_channel', 'pci': 'n_pci', 'siteid': 'n_siteid',
                 'cellid':'n_cellid','azimuth':'n_azimuth','hbwd':'n_hbwd','indoor':'n_indoor','height':'n_height'
                ,'etilt':'n_etilt','mtilt':'n_mtilt','lon':'n_lon','lat':'n_lat','band':'n_band',"contractor":"n_contractor","vendor":"n_vendor"},inplace=True)

    site["eci"] = site[["siteid","cellid"]].apply(lambda x:x["siteid"]*256+x["cellid"],axis=1)
    nsite["n_eci"] = nsite[["n_siteid","n_cellid"]].apply(lambda x:x["n_siteid"]*256+x["n_cellid"],axis=1)

    ho_fail_list=pd.merge(ho_fail_list,site,left_on='rel_id',right_on='eci',how='left')
    ho_fail_list=pd.merge(ho_fail_list,nsite,left_on='rel_id2',right_on='n_eci',how='left')
    for i in range(ho_fail_list.shape[0]):
        ##判断同系统异系统
        if not pd.isnull(ho_fail_list.loc[i,'n_rattype']):
            if ho_fail_list.loc[i,'n_rattype']!='4G':
                ho_fail_list.loc[i,'irat_flag']="异系统切换";
                ho_fail_list.loc[i, 'intrafrequency_flag'] = "-";
            else:
                ho_fail_list.loc[i, 'irat_flag'] = "系统内切换";
                if (not pd.isnull(ho_fail_list.loc[i,'band'])) and  (not pd.isnull(ho_fail_list.loc[i,'n_band'])):
                    if ho_fail_list.loc[i,'band'] != ho_fail_list.loc[i,'n_band']:
                        ho_fail_list.loc[i, 'intrafrequency_flag'] = "异频切换";
                    else:
                        ho_fail_list.loc[i, 'intrafrequency_flag'] = "同频切换";
                else:
                    ho_fail_list.loc[i, 'intrafrequency_flag'] = "无法判断";
        else:
            ho_fail_list.loc[i, 'irat_flag'] = "无法判断";
            ho_fail_list.loc[i, 'intrafrequency_flag'] = "无法判断";
        ##判断室内室外
        if (not pd.isnull(ho_fail_list.loc[i,'indoor'])) and (not pd.isnull(ho_fail_list.loc[i,'n_indoor'])) :
            if ho_fail_list.loc[i,'indoor']==1 and ho_fail_list.loc[i,'n_indoor']==1:
                ho_fail_list.loc[i, 'indoor_outdoor_ho_type'] = "室内切室内";
            elif ho_fail_list.loc[i,'indoor']==0 and ho_fail_list.loc[i,'n_indoor']==0:
                ho_fail_list.loc[i, 'indoor_outdoor_ho_type'] = "室外切室外";
            elif ho_fail_list.loc[i,'indoor']==1 and ho_fail_list.loc[i,'n_indoor']==0:
                ho_fail_list.loc[i, 'indoor_outdoor_ho_type'] = "室内切室外";
            elif ho_fail_list.loc[i,'indoor']==0 and ho_fail_list.loc[i,'n_indoor']==1:
                ho_fail_list.loc[i, 'indoor_outdoor_ho_type'] = "室外切室内";
            else:
                ho_fail_list.loc[i, 'indoor_outdoor_ho_type'] = "无法判断";
        else:
            ho_fail_list.loc[i, 'indoor_outdoor_ho_type'] = "无法判断";
        ##判断厂家
        if (not pd.isnull(ho_fail_list.loc[i,'vendor'])) and (not pd.isnull(ho_fail_list.loc[i,'n_vendor'])) :
            if ho_fail_list.loc[i,'vendor']!=ho_fail_list.loc[i,'n_vendor']:
                ho_fail_list.loc[i, 'ho_vendor_type'] = "异厂家_"+ho_fail_list.loc[i,'vendor']+"_"+ho_fail_list.loc[i,'n_vendor'];
            else:
                ho_fail_list.loc[i, 'ho_vendor_type'] = "同厂家_"+ho_fail_list.loc[i,'vendor']+"_"+ho_fail_list.loc[i,'n_vendor'];
        else:
            ho_fail_list.loc[i, 'ho_vendor_type'] = "无法判断";

        ##判断承建方
        if (not pd.isnull(ho_fail_list.loc[i, 'contractor'])) and (
        not pd.isnull(ho_fail_list.loc[i, 'n_contractor'])):
            if ho_fail_list.loc[i, 'contractor'] != ho_fail_list.loc[i, 'n_contractor']:
                ho_fail_list.loc[i, 'ho_contractor_type'] = "不同运营商_"+ho_fail_list.loc[i, 'contractor']+"_"+ho_fail_list.loc[i, 'n_contractor'];
            else:
                ho_fail_list.loc[i, 'ho_contractor_type'] = "相同运营商_"+ho_fail_list.loc[i, 'contractor'];
        else:
            ho_fail_list.loc[i, 'ho_contractor_type'] = "无法判断";


            ###距离计算
        if (not pd.isnull(ho_fail_list.loc[i,"lon"])) and (not pd.isnull(ho_fail_list.loc[i,"lat"])) and \
                (not pd.isnull(ho_fail_list.loc[i,"n_lon"])) and (not pd.isnull(ho_fail_list.loc[i,"n_lat"])) :
            ho_fail_list.loc[i,"dis_srcell_to_tarcell"]= cf.calc_greate_circle_distance([ho_fail_list.loc[i,"lon"],ho_fail_list.loc[i,"lat"]],
                                                                                     [ho_fail_list.loc[i, "n_lon"],ho_fail_list.loc[i, "n_lat"]])

    return ho_fail_list

###告警判断
def alarm_correlation(ho_fail_list,alarm):
    ho_fail_list["ho_fail_day_num"] = 0
    ho_fail_list["ho_fail_and_alarm_day_num"] = 0
    ho_fail_list["n_ho_fail_and_alarm_day_num"] = 0


    alarm["alarm_starttime"] = alarm["alarm_starttime"].apply(lambda x:x.replace("/","-"))
    alarm["alarm_starttime_unix"] = alarm["alarm_starttime"].apply(\
         lambda x: time.mktime(time.strptime(x, '%Y-%m-%d %H:%M')))  #转linux时间
    alarm["alarm_endtime"] = alarm["alarm_endtime"].apply(lambda x:x.replace("/","-"))
    alarm["alarm_endtime"] = alarm["alarm_endtime"].apply(lambda x:"2022-7-5 0:0" if not len(x) else x)
    alarm["alarm_endtime_unix"] = alarm["alarm_endtime"].apply(\
         lambda x: time.mktime(time.strptime(x, '%Y-%m-%d %H:%M')))  #转linux时间


    for i in range(ho_fail_list.shape[0]):
        alarm_temp = alarm[alarm["enbid"] == ho_fail_list.loc[i, "siteid"]]
        n_alarm_temp = alarm[alarm["enbid"] == ho_fail_list.loc[i, "n_siteid"]]

        if alarm_temp.empty:
            pass
        else:
            ##判断切换失败当日是否有告警
            date = ho_fail_list.loc[i,"date_on_list"]
            date_list = date.split(",")
            for day in date_list:
                if day.endswith('1'):  ###当日数据异常
                    ho_fail_list.loc[i,"ho_fail_day_num"]=ho_fail_list.loc[i,"ho_fail_day_num"]+1
                    day_list=day.split("=")
                    temp = day_list[0]
                    yy = int(temp[0:4])
                    mm = int(temp[4:6])
                    dd = int(temp[6:8])
                    date_time = time.mktime(time.strptime(str(yy)+"-"+str(mm)+"-"+str(dd), "%Y-%m-%d"))  ##转unix时间 单位s
                    alarm_time = 0
                    sub_alarm = alarm_temp[((alarm_temp["alarm_starttime_unix"]-date_time)<86400)& (alarm_temp["alarm_starttime_unix"]>date_time)]
                    ###筛选出当日告警
                    sub_alarm.index=range(sub_alarm.shape[0])
                    for j in range(sub_alarm.shape[0]):
                        if sub_alarm.loc[j,"alarm_starttime_unix"]<date_time:
                            alarm_time = sub_alarm.loc[j,"alarm_endtime_unix"]-date_time +alarm_time;
                        elif sub_alarm.loc[j,"alarm_starttime_unix"]>date_time and (sub_alarm.loc[j,"alarm_starttime_unix"]-date_time)<86400:
                            alarm_time = sub_alarm.loc[j,"alarm_endtime_unix"] - sub_alarm.loc[j,"alarm_starttime_unix"] + alarm_time;
                        else:
                            continue
                    #当日告警时长＞半小时，判断为异常
                    if alarm_time > 1800:
                        ho_fail_list.loc[i, "ho_fail_and_alarm_day_num"] = ho_fail_list.loc[
                                                                               i, "ho_fail_and_alarm_day_num"] + 1

        if n_alarm_temp.empty:
            pass
        else:
            ##判断切换失败当日是否有告警
            date = ho_fail_list.loc[i,"date_on_list"]
            date_list = date.split(",")
            for day in date_list:
                if day.endswith('1'):  ###当日数据异常
                    ho_fail_list.loc[i,"ho_fail_day_num"]=ho_fail_list.loc[i,"ho_fail_day_num"]+1
                    day_list=day.split("=")
                    temp = day_list[0]
                    yy = int(temp[0:4])
                    mm = int(temp[4:6])
                    dd = int(temp[6:8])
                    date_time = time.mktime(time.strptime(str(yy)+"-"+str(mm)+"-"+str(dd), "%Y-%m-%d"))  ##转unix时间 单位s
                    alarm_time = 0
                    sub_alarm = n_alarm_temp[((n_alarm_temp["alarm_starttime_unix"]-date_time)<86400)& (n_alarm_temp["alarm_starttime_unix"]>date_time)]
                    ###筛选出当日告警
                    sub_alarm.index=range(sub_alarm.shape[0])
                    for j in range(sub_alarm.shape[0]):
                        if sub_alarm.loc[j,"alarm_starttime_unix"]<date_time:
                            alarm_time = sub_alarm.loc[j,"alarm_endtime_unix"]-date_time +alarm_time;
                        elif sub_alarm.loc[j,"alarm_starttime_unix"]>date_time and (sub_alarm.loc[j,"alarm_starttime_unix"]-date_time)<86400:
                            alarm_time = sub_alarm.loc[j,"alarm_endtime_unix"] - sub_alarm.loc[j,"alarm_starttime_unix"] + alarm_time;
                        else:
                            continue
                    #当日告警时长＞半小时，判断为异常
                    if alarm_time > 1800:
                        ho_fail_list.loc[i, "n_ho_fail_and_alarm_day_num"] = ho_fail_list.loc[
                                                                               i, "n_ho_fail_and_alarm_day_num"] + 1


    return ho_fail_list
