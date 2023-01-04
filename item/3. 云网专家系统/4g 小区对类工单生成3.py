# coding=utf-8
import pandas as pd
import math
import psycopg2
import numpy as np
import datetime
# 显示所有列
pd.set_option('display.max_columns',None)
# 显示所有行
pd.set_option('display.max_rows',None)



def calc_greate_circle_distance(lola1,lola2):
    lat1 = lola1[1]
    lon1 = lola1[0]
    lat2 = lola2[1]
    lon2 = lola2[0]
    R=6371000
    lat1_rad = lat1 * math.pi / 180
    lon1_rad = lon1 * math.pi / 180
    lat2_rad = lat2 * math.pi / 180
    lon2_rad = lon2 * math.pi / 180
    d = math.sqrt(math.pow(math.cos((lat1_rad + lat2_rad) / 2) * R * (lon1_rad - lon2_rad), 2) +
                  math.pow(R * (lat1_rad - lat2_rad), 2))
    if d < 10000:
        return round(float(d),2)
    else:
        delta_lat = lat2_rad - lat1_rad
        delta_lon = lon2_rad - lon1_rad
        a = math.sin(delta_lat / 2) * math.sin(delta_lat / 2) + \
            math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) * math.sin(delta_lon / 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        return round(float(R * c),2)

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
    sql_problem ="select order_code,fault_subclass,cell_name,lon,lat,rel_id,tuning_action,root_cause,comments,rel_id2,date_on_list,\
order_sorting,order_date from cnio.fault_workorder where order_date between %s and %s and fault_subclass = %s"
    # 查询taskid
    cur.execute(sql_problem,sql_parameter)
    ho_fail_list = cur.fetchall()
    if len(ho_fail_list) == 0:
        pass
    else:
        ho_fail_list = pd.DataFrame(ho_fail_list)
        # 表描述
        des = cur.description
        # 获取表头
        colname = []
        for item in des:
            colname.append(item[0])
        ho_fail_list.columns = colname

    # 查询pm指标
    sql_pm = "\
select trace_date as pm_date,eci,dl_busy_prb_use_ratio,ul_busy_prb_use_ratio,\
idletime_prb_noise_power_dbm as ul_noise from cnio.pm_4g_workorder pgw where trace_date between %s and  %s"
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
    cell_pm.loc[:,'pm_date'] = cell_pm['pm_date'].astype(str)

    # 查询mr指标
    sql_mr = "\
select trace_date as mr_date,eci, servingsample,overshootingsample,totalsample,overlap3sample,servrsrpdistr0,\
             servrsrpdistr1,servrsrpdistr2 from cnio.trace_mr_cell tmc  where trace_date between %s and %s"
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
    cell_mr.loc[:,'mr_date']=cell_mr['mr_date'].astype(str)
    # 查询工单是否有工程质量问题
    sql_project_problem_cell = " \
select order_code,rel_id as eci,fault_subclass from cnio.fault_workorder fw \
where fault_class ='工程质量' and fault_catalog ='小区类' and order_status!='04' \
"
    # 查询taskid
    cur.execute(sql_project_problem_cell, sql_date)
    cell_project_problem_list = cur.fetchall()
    if len(cell_project_problem_list) == 0:
        pass
    else:
        cell_project_problem_list = pd.DataFrame(cell_project_problem_list)
        # 表描述
        des = cur.description
        # 获取表头
        colname = []
        for item in des:
            colname.append(item[0])
        cell_project_problem_list.columns = colname

    #查询告警数据
    sql_parameter_date = [start_day,end_day,start_day,end_day]
#     sql_alarm = "\
# select enbid,alarm_starttime,alarm_endtime from cnio.alarm_detail dad where enbid <> '		-' \
# and  (dad.vendor='华为' and (dad.alarm_content='网元连接中断' or dad.alarm_content='小区不可用告警' \
# or dad.alarm_content='射频单元链路维护异常告警' or dad.alarm_content='射频单元硬件故障告警'  or dad.alarm_content='射频单元直流掉电告警'  or dad.alarm_content='射频单元业务不可用告警'  or dad.alarm_content='射频单元CPRI接口异常告警' \
# or dad.alarm_content='传输光模块故障告警'  or dad.alarm_content='传输光接口异常告警'   or dad.alarm_content='RHUB光模块故障告警' \
# or dad.alarm_content='RHUB光模块/电接口不在位告警'\
# or dad.alarm_content='RHUB与pRRU间链路异常告警'  or dad.alarm_content='RHUB CPRI接口异常告警'  \
# or dad.alarm_content='BBU CPRI接口异常告警'  or dad.alarm_content='RHUB CPRI接口异常告警'  or dad.alarm_content='BBU CPRI光模块/电接口不在位告警'  \
# or dad.alarm_content='LTE小区退出服务(198094419)'  or dad.alarm_content='网元断链告警(198099803)'  \
# or dad.alarm_content='天馈驻波比异常(198098465)' \
# or dad.alarm_content='RRU链路断(198097605)' \
# or dad.alarm_content='设备掉电(198092295)'   or dad.alarm_content='S1断链告警(198094420)'\
# or dad.alarm_content='市电停电告警(198092550)') or (dad.vendor='中兴' and  dad.importance='严重') )"
    sql_alarm="\
    select data_time as alarm_date,eci,important_alarm_num,urgent_alarm_num,top1_alarm_name from cnio.alarm_hz_day dad where (data_time between %s and %s) \
    and (((dad.important_alarm_num is not null) and (dad.important_alarm_num != 0)) or ((dad.urgent_alarm_num is not null) and (dad.urgent_alarm_num != 0)))"

    # 查询taskid
    cur.execute(sql_alarm,sql_date)
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
        cell_alarm.loc[:,'alarm_date'] = cell_alarm['alarm_date'].astype(str)

    # 工参查询
    sql_siteinfo= "\
select rattype,channel,pci,siteid,cellid,ci,azimuth,hbwd,indoor,height ,etilt,mtilt,lon,lat,\
band,contractor,vendor  from cnio.siteinfo s where data_date = (select max(data_date) from cnio.siteinfo )\
"
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

    return ho_fail_list,cell_pm,cell_mr,cell_project_problem_list,cell_alarm,siteinfo

def ho_diagnosis(ho_fail_list):

    ho_fail_list.loc[:,"order_sorting"] = ""
    ho_fail_list.loc[:,"root_cause"] = ""
    ho_fail_list.loc[:,"tuning_action"] = ""
    ho_fail_list.loc[:,"comments"] = ""
    #ho_fail_list[["ul_noise", "n_ul_noise"]] = ho_fail_list[["ul_noise", "n_ul_noise"]].fillna(-120)
    #ho_fail_list.fillna(-1,inplace=True)

    for i in range(ho_fail_list.shape[0]):
        root_cause_l = 0
        ho_fail_list.loc[i, "comments"]='【切换类型画像】系统内/间:'+ho_fail_list.loc[i,'irat_flag']+',同/异频:'\
                                        +ho_fail_list.loc[i, 'intrafrequency_flag']+',室内外:'\
                                        +ho_fail_list.loc[i, 'indoor_outdoor_ho_type']+',设备商:'\
                                        +ho_fail_list.loc[i, 'ho_contractor_type']+',切换距离:'\
                                        +str(ho_fail_list.loc[i, 'dis_srcell_to_tarcell'])+'米'
        if ho_fail_list.loc[i, 'dis_srcell_to_tarcell']!='-':
            if ho_fail_list.loc[i, 'dis_srcell_to_tarcell']>800:
                ho_fail_list.loc[i, "comments"]=ho_fail_list.loc[i, "comments"]+'(切换距离较远)。'+'$'
            else:
                ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + '。' + '$'
        else:
            ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + '。' + '$'

        if (ho_fail_list.loc[i, "ho_fail_alarm_day_num"] >= 1) or (ho_fail_list.loc[i, "n_ho_fail_alarm_day_num"] >= 1):

            root_cause_l=root_cause_l+1
            ho_fail_list.loc[i, "order_sorting"] = "维护"
            ho_fail_list.loc[i, "root_cause"] = str(root_cause_l)+".【紧急/重要告警】"
            ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l)+'.告警排查。'

            alarm_date_list = ho_fail_list.loc[i, "ho_fail_alarm_day"].split(",")
            alarm_num_list = ho_fail_list.loc[i, "ho_fail_alarm_num"].split(",")
            alarm_name_list = ho_fail_list.loc[i, "ho_fail_top1_alarm_name"].split(",")

            alarm_date_list1 = ho_fail_list.loc[i, "n_ho_fail_alarm_day"].split(",")
            alarm_num_list1= ho_fail_list.loc[i, "n_ho_fail_alarm_num"].split(",")
            alarm_name_list1 = ho_fail_list.loc[i, "n_ho_fail_top1_alarm_name"].split(",")
            # if ho_fail_list.loc[i, "order_code"]==2022111324439:
            #     print(ho_fail_list.loc[i,:])
            if (ho_fail_list.loc[i, "ho_fail_alarm_day_num"] >= 1) and (ho_fail_list.loc[i, "n_ho_fail_alarm_day_num"] >= 1):
                if ho_fail_list.loc[i, "ho_fail_alarm_day_num"] == 1:
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(root_cause_l) + \
                                                          '.【源小区告警信息】' + alarm_date_list[0] + ',' + \
                                                          alarm_num_list[0] + '次，TOP1告警名称:' + alarm_name_list[
                                                              0] + '。$'
                else:
                    for o in range(len(alarm_date_list)):
                        if o==0:
                            ho_fail_list.loc[i, "comments"] =  ho_fail_list.loc[i, "comments"]+str(root_cause_l)+\
                                                               '.【源小区告警信息】'+ alarm_date_list[o]+','+ alarm_num_list[o]+'次，TOP1告警名称:'+alarm_name_list[o]+';'
                        elif o==len(alarm_date_list)-1:
                            ho_fail_list.loc[i, "comments"]= ho_fail_list.loc[i, "comments"]+ alarm_date_list[o]+','+ alarm_num_list[o]+'次，TOP1告警名称:'+alarm_name_list[o]+'。'+'$'
                        else:
                            ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + alarm_date_list[o] + ',' + \
                                                              alarm_num_list[o] + '次，TOP1告警名称:' + alarm_name_list[o] + ';'

                if ho_fail_list.loc[i, "n_ho_fail_alarm_day_num"] == 1:
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"]  + \
                                                          '【目标小区告警信息】' + alarm_date_list1[0] + ',' + \
                                                          alarm_num_list1[0] + '次，TOP1告警名称:' + alarm_name_list1[
                                                              0] + '。$'
                else:
                    for o in range(len(alarm_date_list1)):
                        if o==0:
                            ho_fail_list.loc[i, "comments"] =  ho_fail_list.loc[i, "comments"]+\
                                                               '【目标小区告警信息】'+ alarm_date_list1[o]+','+ alarm_num_list1[o]+'次，TOP1告警名称:'+alarm_name_list1[o]+';'
                        elif o==len(alarm_date_list)-1:
                            ho_fail_list.loc[i, "comments"]= ho_fail_list.loc[i, "comments"]+ alarm_date_list1[o]+','+ alarm_num_list1[o]+'次，TOP1告警名称:'+alarm_name_list1[o]+'。'+'$'
                        else:
                            ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + alarm_date_list1[o] + ',' + \
                                                              alarm_num_list1[o] + '次，TOP1告警名称:' + alarm_name_list1[o] + ';'

            if (ho_fail_list.loc[i, "ho_fail_alarm_day_num"] >= 1) and (ho_fail_list.loc[i, "n_ho_fail_alarm_day_num"] <1):
                if ho_fail_list.loc[i, "ho_fail_alarm_day_num"] == 1:
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(root_cause_l) + \
                                                          '.【源小区告警信息】' + alarm_date_list[0] + ',' + \
                                                          alarm_num_list[0] + '次，TOP1告警名称:' + alarm_name_list[
                                                              0] + '。$'
                else:
                    for o in range(len(alarm_date_list)):
                        if o==0:
                            ho_fail_list.loc[i, "comments"] =  ho_fail_list.loc[i, "comments"]+str(root_cause_l)+\
                                                               '.【源小区告警信息】'+ alarm_date_list[o]+','+ alarm_num_list[o]+'次，TOP1告警名称:'+alarm_name_list[o]+';'
                        elif o==len(alarm_date_list)-1:
                            ho_fail_list.loc[i, "comments"]= ho_fail_list.loc[i, "comments"]+ alarm_date_list[o]+','+ alarm_num_list[o]+'次，TOP1告警名称:'+alarm_name_list[o]+'。'+'$'
                        else:
                            ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + alarm_date_list[o] + ',' + \
                                                              alarm_num_list[o] + '次，TOP1告警名称:' + alarm_name_list[o] + ';'

            if (ho_fail_list.loc[i, "ho_fail_alarm_day_num"] <1) and (
                    ho_fail_list.loc[i, "n_ho_fail_alarm_day_num"] >=1):
                if ho_fail_list.loc[i, "n_ho_fail_alarm_day_num"] == 1:
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(root_cause_l) + \
                                                          '.【目标小区告警信息】' + alarm_date_list1[0] + ',' + \
                                                          alarm_num_list1[0] + '次，TOP1告警名称:' + alarm_name_list1[
                                                              0] + '。$'
                else:
                    for o in range(len(alarm_date_list1)):
                        if o == 0:
                            ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(root_cause_l) + \
                                                              '.【目标小区告警信息】' + alarm_date_list1[o] + ',' + \
                                                              alarm_num_list1[o] + '次，TOP1告警名称:' + \
                                                              alarm_name_list1[o] + ';'
                        elif o == len(alarm_date_list) - 1:
                            ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + alarm_date_list1[
                                o] + ',' + alarm_num_list1[o] + '次，TOP1告警名称:' + alarm_name_list1[o] + '。' + '$'
                        else:
                            ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + alarm_date_list1[
                                o] + ',' + \
                                                              alarm_num_list1[o] + '次，TOP1告警名称:' + \
                                                              alarm_name_list1[o] + ';'


        # if ho_fail_list.loc[i,"order_sorting"]=="":
        #     if  pd.isnull(ho_fail_list.loc[i,"neighbour_flag"]) :
        #         ho_fail_list.loc[i,"order_sorting"] = "优化"
        #         ho_fail_list.loc[i,"root_cause"] = "邻区漏配"
        #         ho_fail_list.loc[i,"tuning_action"] = "配置源小区到目标小区的邻区关系"
        # if ho_fail_list.loc[i, "order_sorting"] == "":
        #     if not pd.isnull(ho_fail_list.loc[i, "pci_confuse_eci"]):
        #         ho_fail_list.loc[i, "order_sorting"] = "优化"
        #         ho_fail_list.loc[i, "root_cause"] = "目标小区pci混淆"
        #         ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + "重配置目标小区及混淆邻区的pci"
#工程质量问题关联
        if len(ho_fail_list.loc[i, "src_project_problem_flag"]) != 0 and len(
            ho_fail_list.loc[i, "tar_project_problem_flag"]) != 0:
            if root_cause_l==0:
                ho_fail_list.loc[i, "order_sorting"] = "优化"
            root_cause_l = root_cause_l + 1
            if root_cause_l < 4:
                ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"]+str(root_cause_l)+".【工程质量问题】"
                ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l)+".排查源及目标小区工程质量问题。"
                ho_fail_list.loc[i, "comments"]=ho_fail_list.loc[i, "comments"]+str(root_cause_l)+'.【工程质量问题工单关联】'+\
                    '源小区:'+ho_fail_list.loc[i, "src_project_problem_flag"]+';'+'目标小区:'+ho_fail_list.loc[i, "tar_project_problem_flag"]+'。'+'$'

        elif len(ho_fail_list.loc[i, "src_project_problem_flag"]) != 0 and len(
            ho_fail_list.loc[i, "tar_project_problem_flag"]) == 0:
            if root_cause_l==0:
                ho_fail_list.loc[i, "order_sorting"] = "优化"
            root_cause_l = root_cause_l + 1
            if root_cause_l < 4:
                ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"]+str(root_cause_l)+".【工程质量问题】"
                ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l)+".排查源小区工程质量问题。"
                ho_fail_list.loc[i, "comments"]=ho_fail_list.loc[i, "comments"]+str(root_cause_l)+'.【工程质量问题工单关联】'+\
                    '源小区:'+ho_fail_list.loc[i, "src_project_problem_flag"]+'。'+'$'

        elif len(ho_fail_list.loc[i, "src_project_problem_flag"]) == 0 and len(
                ho_fail_list.loc[i, "tar_project_problem_flag"]) != 0:
            if root_cause_l==0:
                ho_fail_list.loc[i, "order_sorting"] = "优化"
            root_cause_l = root_cause_l + 1
            if root_cause_l < 4:
                ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"]+str(root_cause_l)+".【工程质量问题】"
                ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l)+".排查目标小区工程质量问题。"
                ho_fail_list.loc[i, "comments"]=ho_fail_list.loc[i, "comments"]+str(root_cause_l)+'.【工程质量问题工单关联】'+\
                    '目标小区:'+ho_fail_list.loc[i, "tar_project_problem_flag"]+'。'+'$'

#排查是否存在干扰情况
        if (not pd.isnull(ho_fail_list.loc[i, "ul_noise"])) and (
        not pd.isnull(ho_fail_list.loc[i, "n_ul_noise"])):
            if ho_fail_list.loc[i, "ul_noise"] >= -95 and ho_fail_list.loc[i, "n_ul_noise"] >= -95:
                if root_cause_l == 0:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                root_cause_l =root_cause_l +1
                if root_cause_l < 4:
                    ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"]+str(root_cause_l)+".【存在干扰】"
                    ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l)+".排查源及目标小区上行干扰。"
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(root_cause_l) + '.【干扰问题描述】' + \
                                                      '源小区闲时平均每PRB干扰噪声功率:' + str(ho_fail_list.loc[
                                                          i, "ul_noise"]) + 'dBm;' + '目标小区闲时平均每PRB干扰噪声功率:' + str(ho_fail_list.loc[
                                                          i, "n_ul_noise"]) + 'dBm。'+'$'

            elif ho_fail_list.loc[i, "ul_noise"] >= -95 and ho_fail_list.loc[i, "n_ul_noise"] < -95:
                if root_cause_l == 0:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                root_cause_l = root_cause_l + 1
                if root_cause_l < 4:
                    ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"]+str(root_cause_l)+".【存在干扰】"
                    ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l)+".排查源小区上行干扰。"
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(root_cause_l) + '.【干扰问题描述】' + \
                                                      '源小区闲时平均每PRB干扰噪声功率:' + str(ho_fail_list.loc[
                                                          i, "ul_noise"]) + 'dBm。'+'$'

            elif ho_fail_list.loc[i, "ul_noise"] < -95 and ho_fail_list.loc[i, "n_ul_noise"] >= -95:
                if root_cause_l == 0:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                root_cause_l = root_cause_l + 1
                if root_cause_l < 4:
                    ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"] + str(root_cause_l) + ".【存在干扰】"
                    ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l) + ".排查目标小区上行干扰。"
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(root_cause_l) + '.【干扰问题描述】' + \
                                                       '目标小区闲时平均每PRB干扰噪声功率:' +  str(ho_fail_list.loc[ i, "n_ul_noise"]) + 'dBm。'+'$'

        elif (not pd.isnull(ho_fail_list.loc[i, "ul_noise"])) and (pd.isnull(ho_fail_list.loc[i, "n_ul_noise"])):
            if ho_fail_list.loc[i, "ul_noise"] >= -95:
                if root_cause_l == 0:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                root_cause_l = root_cause_l + 1
                if root_cause_l < 4:
                    ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"]+str(root_cause_l)+".【存在干扰】"
                    ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l)+".排查源小区上行干扰。"
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(root_cause_l) + '.【干扰问题描述】' + \
                                                      '源小区闲时平均每PRB干扰噪声功率:' + str(ho_fail_list.loc[
                                                          i, "ul_noise"]) + 'dBm。'+'$'

        elif (pd.isnull(ho_fail_list.loc[i, "ul_noise"])) and (not pd.isnull(ho_fail_list.loc[i, "n_ul_noise"])):
            if ho_fail_list.loc[i, "n_ul_noise"] >= -95:
                if root_cause_l == 0:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                root_cause_l = root_cause_l + 1
                if root_cause_l < 4:
                    ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"]+str(root_cause_l)+".【存在干扰】"
                    ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l)+".排查目标小区上行干扰。"
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(root_cause_l) + '.【干扰问题描述】' + \
                                                     '目标小区闲时平均每PRB干扰噪声功率:' + str(ho_fail_list.loc[i, "n_ul_noise"]) + 'dBm。'+'$'
#MR覆盖排查（弱覆盖.越区.重叠）
#弱覆盖
        if (not pd.isnull(ho_fail_list.loc[i, "rsrpge110_ratio"])) and (
        not pd.isnull(ho_fail_list.loc[i, "n_rsrpge110_ratio"])):
            if ho_fail_list.loc[i, "rsrpge110_ratio"] < 0.7 and ho_fail_list.loc[
                i, "n_rsrpge110_ratio"] < 0.7:
                if root_cause_l == 0:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                root_cause_l = root_cause_l + 1
                if root_cause_l < 4:
                    ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"]+str(root_cause_l)+".【弱覆盖】"
                    ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l)+".优化提升源及目标小区的覆盖质量。"
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(root_cause_l) + '.【弱覆盖问题描述】' + \
                                                      '源小区大于-110dbm的采样点比例:' + str(round(ho_fail_list.loc[
                                                          i, "rsrpge110_ratio"]*100,2)) + '%;' + '目标小区大于-110dbm的采样点比例:' + \
                                                      str(round(ho_fail_list.loc[
                                                          i, "n_rsrpge110_ratio"]*100,2)) + '%。'+'$'

            elif ho_fail_list.loc[i, "rsrpge110_ratio"] < 0.7 and ho_fail_list.loc[
                i, "n_rsrpge110_ratio"] >= 0.7:
                if root_cause_l == 0:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                root_cause_l = root_cause_l + 1
                if root_cause_l < 4:
                    ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"]+str(root_cause_l)+".【弱覆盖】"
                    ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l)+".优化提升源小区的覆盖质量。"
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(root_cause_l) + '.【弱覆盖问题描述】' + \
                                                      '源小区大于-110dbm的采样点比例:' + str(round(ho_fail_list.loc[
                                                          i, "rsrpge110_ratio"]*100,2)) + '%。'+'$'

            elif ho_fail_list.loc[i, "rsrpge110_ratio"] >= 0.7 and ho_fail_list.loc[
                i, "n_rsrpge110_ratio"] < 0.7:
                if root_cause_l == 0:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                root_cause_l = root_cause_l + 1
                if root_cause_l < 4:
                    ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"]+str(root_cause_l)+".【弱覆盖】"
                    ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l)+".优化提升目标小区的覆盖质量。"
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(root_cause_l) + '.【弱覆盖问题描述】' + \
                                                      '目标小区大于-110dbm的采样点比例:' + str(round(ho_fail_list.loc[
                                                          i, "n_rsrpge110_ratio"]*100,2)) + '%。'+'$'

        elif (not pd.isnull(ho_fail_list.loc[i, "rsrpge110_ratio"])) and (
        pd.isnull(ho_fail_list.loc[i, "n_rsrpge110_ratio"])):
            if ho_fail_list.loc[i, "rsrpge110_ratio"] < 0.7:
                if root_cause_l == 0:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                root_cause_l = root_cause_l + 1
                if root_cause_l < 4:
                    ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"] + str(root_cause_l) + ".【弱覆盖】"
                    ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l) + ".优化提升源小区的覆盖质量。"
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(root_cause_l) + '.【弱覆盖问题描述】' + \
                                                      '源小区大于-110dbm的采样点比例:' + str(round(ho_fail_list.loc[
                                                                                      i, "rsrpge110_ratio"] * 100,2)) + '%。'+'$'

        elif (pd.isnull(ho_fail_list.loc[i, "rsrpge110_ratio"])) and (
        not pd.isnull(ho_fail_list.loc[i, "n_rsrpge110_ratio"])):
            if ho_fail_list.loc[i, "n_rsrpge110_ratio"] < 0.7:
                if root_cause_l == 0:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                root_cause_l = root_cause_l + 1
                if root_cause_l < 4:
                    ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"] + str(root_cause_l) + ".【弱覆盖】"
                    ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l) + ".优化提升目标小区的覆盖质量。"
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(root_cause_l) + '.【弱覆盖问题描述】' + \
                                                      '目标小区大于-110dbm的采样点比例:' + str(round(ho_fail_list.loc[
                                                                                       i, "n_rsrpge110_ratio"] * 100,2)) + '%。'+'$'

#越区覆盖
        if (not pd.isnull(ho_fail_list.loc[i, "overshooting_ratio"])) and (
        not pd.isnull(ho_fail_list.loc[i, "n_overshooting_ratio"])):
            if ho_fail_list.loc[i, "overshooting_ratio"] >= 0.05 and ho_fail_list.loc[
                i, "n_overshooting_ratio"] >= 0.05:
                if root_cause_l == 0:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                root_cause_l = root_cause_l + 1
                if root_cause_l < 4:
                    ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"]+str(root_cause_l)+".【越区覆盖】"
                    ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l)+".控制源及目标小区的覆盖范围。"
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(root_cause_l) + '.【越区覆盖问题描述】' + \
                                                      '源小区越区覆盖采样点比例:' + str(round(ho_fail_list.loc[
                                                          i, "overshooting_ratio"]*100,2)) + '%;' + '目标小区越区覆盖采样点比例:' + \
                                                      str(round(ho_fail_list.loc[
                                                          i, "n_overshooting_ratio"]*100,2)) + '%。'+'$'

            elif ho_fail_list.loc[i, "overshooting_ratio"] >= 0.05 and ho_fail_list.loc[
                i, "n_overshooting_ratio"] < 0.05:
                if root_cause_l == 0:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                root_cause_l = root_cause_l + 1
                if root_cause_l < 4:
                    ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"] + str(
                        root_cause_l) + ".【越区覆盖】"
                    ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l) + ".控制源小区的覆盖范围。"
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(
                        root_cause_l) + '.【越区覆盖问题描述】' + \
                                                      '源小区越区覆盖采样点比例:' + str(round(ho_fail_list.loc[
                                                                                i, "overshooting_ratio"] * 100,2)) + '%。'+'$'

            elif ho_fail_list.loc[i, "overshooting_ratio"] < 0.05 and ho_fail_list.loc[
                i, "n_overshooting_ratio"] >= 0.05:
                if root_cause_l == 0:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                root_cause_l = root_cause_l + 1
                if root_cause_l < 4:
                    ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"] + str(
                        root_cause_l) + ".【越区覆盖】"
                    ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] +ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l) + ".控制目标小区的覆盖范围。"
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(
                        root_cause_l) + '.【越区覆盖问题描述】' + \
                                                      '目标小区越区覆盖采样点比例:' + str(round(ho_fail_list.loc[
                                                                                i, "n_overshooting_ratio"] * 100,2)) + '%。'+'$'

        elif (not pd.isnull(ho_fail_list.loc[i, "overshooting_ratio"])) and (
        pd.isnull(ho_fail_list.loc[i, "n_overshooting_ratio"])):
            if ho_fail_list.loc[i, "overshooting_ratio"] >= 0.05:
                if root_cause_l == 0:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                root_cause_l = root_cause_l + 1
                if root_cause_l < 4:
                    ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"] + str(
                        root_cause_l) + ".【越区覆盖】"
                    ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] +ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l) + ".控制源小区的覆盖范围。"
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(
                        root_cause_l) + '.【越区覆盖问题描述】' + \
                                                      '源小区越区覆盖采样点比例:' + str(round(ho_fail_list.loc[
                                                                                i, "overshooting_ratio"] * 100,2)) + '%。'+'$'

        elif (pd.isnull(ho_fail_list.loc[i, "overshooting_ratio"])) and (
        not pd.isnull(ho_fail_list.loc[i, "n_overshooting_ratio"])):
            if ho_fail_list.loc[i, "n_overshooting_ratio"] >= 0.05:
                if root_cause_l == 0:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                root_cause_l = root_cause_l + 1
                if root_cause_l < 4:
                    ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"] + str(
                        root_cause_l) + ".【越区覆盖】"
                    ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] +ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l) + ".控制目标小区的覆盖范围。"
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(
                        root_cause_l) + '.【越区覆盖问题描述】' + \
                                                      '目标小区越区覆盖采样点比例:' + str(round(ho_fail_list.loc[
                                                                                 i, "n_overshooting_ratio"] * 100,2)) + '%。'+'$'

#重叠覆盖
        if (not pd.isnull(ho_fail_list.loc[i, "overlap_ratio"])) and (
        not pd.isnull(ho_fail_list.loc[i, "n_overlap_ratio"])):
            if ho_fail_list.loc[i, "overlap_ratio"] >= 0.2 and ho_fail_list.loc[i, "n_overlap_ratio"] >= 0.2:
                if root_cause_l == 0:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                root_cause_l = root_cause_l + 1
                if root_cause_l < 4:
                    ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"] + str(
                        root_cause_l) + ".【重叠覆盖】"
                    ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l) + ".控制源和目标小区及周边邻区覆盖范围。"
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(
                        root_cause_l) + '.【重叠覆盖问题描述】' + \
                                                      '源小区重叠覆盖采样点比例:' + str(round(ho_fail_list.loc[
                                                                                i, "overlap_ratio"] * 100,2)) + '%;' + '目标小区重叠覆盖采样点比例:' + \
                                                      str(round(ho_fail_list.loc[
                                                              i, "n_overlap_ratio"] * 100,2)) + '%。'+'$'

            elif ho_fail_list.loc[i, "overlap_ratio"] >= 0.2 and ho_fail_list.loc[i, "n_overlap_ratio"] < 0.2:
                if root_cause_l == 0:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                root_cause_l = root_cause_l + 1
                if root_cause_l < 4:
                    ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"] + str(
                        root_cause_l) + ".【重叠覆盖】"
                    ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l) + ".控制源小区及周边邻区覆盖范围。"
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(
                        root_cause_l) + '.【重叠覆盖问题描述】' + \
                                                      '源小区重叠覆盖采样点比例:' + str(round(ho_fail_list.loc[
                                                                                i, "overlap_ratio"] * 100,2)) + '%。'+'$'

            elif ho_fail_list.loc[i, "overlap_ratio"] < 0.2 and ho_fail_list.loc[i, "n_overlap_ratio"] >= 0.2:
                if root_cause_l == 0:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                root_cause_l = root_cause_l + 1
                if root_cause_l < 4:
                    ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"] + str(
                        root_cause_l) + ".【重叠覆盖】"
                    ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l) + ".控制目标小区及周边邻区覆盖范围。"
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(
                        root_cause_l) + '.【重叠覆盖问题描述】' + '目标小区重叠覆盖采样点比例:' + str(round(ho_fail_list.loc[
                                                              i, "n_overlap_ratio"] * 100,2)) + '%。'+'$'

        elif (not pd.isnull(ho_fail_list.loc[i, "overlap_ratio"])) and (
        pd.isnull(ho_fail_list.loc[i, "n_overlap_ratio"])):
            if ho_fail_list.loc[i, "overlap_ratio"] >= 0.2:
                if root_cause_l == 0:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                root_cause_l = root_cause_l + 1
                if root_cause_l < 4:
                    ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"] + str(
                        root_cause_l) + ".【重叠覆盖】"
                    ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l) + ".控制源小区及周边邻区覆盖范围。"
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(
                        root_cause_l) + '.【重叠覆盖问题描述】' + \
                                                      '源小区重叠覆盖采样点比例:' + str(round(ho_fail_list.loc[
                                                                                i, "overlap_ratio"] * 100,2)) + '%。'+'$'

        elif (pd.isnull(ho_fail_list.loc[i, "overlap_ratio"])) and (
        not pd.isnull(ho_fail_list.loc[i, "n_overlap_ratio"])):
            if ho_fail_list.loc[i, "n_overlap_ratio"] >= 0.2:
                if root_cause_l == 0:
                    ho_fail_list.loc[i, "order_sorting"] = "优化"
                root_cause_l = root_cause_l + 1
                if root_cause_l < 4:
                    ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"] + str(
                        root_cause_l) + ".【重叠覆盖】"
                    ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l) + ".控制目标小区及周边邻区覆盖范围。"
                    ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(
                        root_cause_l) + '.【重叠覆盖问题描述】' + '目标小区重叠覆盖采样点比例:' + str(round(ho_fail_list.loc[
                                                                                   i, "n_overlap_ratio"] * 100,2)) + '%。'+'$'
#负荷排查

        if (not pd.isnull(ho_fail_list.loc[i, "n_dl_busy_prb_use_ratio"])) and ho_fail_list.loc[
            i, "n_dl_busy_prb_use_ratio"] >= 0.5:
            if root_cause_l == 0:
                ho_fail_list.loc[i, "order_sorting"] = "优化"
            root_cause_l = root_cause_l + 1
            if root_cause_l < 4:
                ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"] + str(
                    root_cause_l) + ".【负荷过高】"
                ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l) + ".核查目标小区负荷，扩容或建设方式解决。"
                ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(
                    root_cause_l) + '.【负荷问题描述】' + '目标小区下行平均忙时PRB利用率:' + str(round(ho_fail_list.loc[
                                                                               i, "n_dl_prb_use_ratio"] * 100,2)) + '%。'+'$'
        if (not pd.isnull(ho_fail_list.loc[i, "n_ul_busy_prb_use_ratio"])) and ho_fail_list.loc[
            i, "n_ul_busy_prb_use_ratio"] >= 0.5:
            if root_cause_l == 0:
                ho_fail_list.loc[i, "order_sorting"] = "优化"
            root_cause_l = root_cause_l + 1
            if root_cause_l < 4:
                ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"] + str(
                    root_cause_l) + ".【负荷过高】"
                ho_fail_list.loc[i, "tuning_action"] = ho_fail_list.loc[i, "tuning_action"] + str(
                    root_cause_l) + ".核查目标小区负荷，扩容或建设方式解决。"
                ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(
                    root_cause_l) + '.【负荷盖问题描述】' + '目标小区上行平均忙时PRB利用率:' + str(round(ho_fail_list.loc[
                                                                                      i, "n_dl_prb_use_ratio"] * 100,
                                                                                  2)) + '%。' + '$'

#未排查出原因
        if root_cause_l == 0:
            ho_fail_list.loc[i, "order_sorting"] = "专家"
            root_cause_l = root_cause_l + 1
            ho_fail_list.loc[i, "root_cause"] = ho_fail_list.loc[i, "root_cause"] + str(
                root_cause_l) + ".【需专家分析】"
            ho_fail_list.loc[i, "tuning_action"] =ho_fail_list.loc[i, "tuning_action"] + str(root_cause_l) + ".网优专家分析。"
            ho_fail_list.loc[i, "comments"] = ho_fail_list.loc[i, "comments"] + str(
                    root_cause_l)+'.【专家分析操作】核查切换参数配置及信令回溯分析。'+'$'

    ho_fail_list = ho_fail_list.loc[:, ~ho_fail_list.columns.isin(["eci_x", "eci_y", "n_eci_x", "n_eci_y"])]


    return ho_fail_list

###工程质量工单关联
def project_problem_correlation(ho_fail_list,cell_project_problem_list):

    ho_fail_list.loc[:,"src_project_problem_flag"]=""
    ho_fail_list.loc[:,"tar_project_problem_flag"] = ""
    if np.array(cell_project_problem_list).shape[0] > 0:
        cell_project_problem_list.index= list(range(cell_project_problem_list.shape[0]))
        for i in range(ho_fail_list.shape[0]):
            for j in range(cell_project_problem_list.shape[0]):
                if ho_fail_list.loc[i, "rel_id"] == cell_project_problem_list.loc[j, "eci"]:
                    if ho_fail_list.loc[i, "src_project_problem_flag"] == "":
                        ho_fail_list.loc[i, "src_project_problem_flag"] = "工单编号-" + str(cell_project_problem_list.loc[
                            j, "order_code"]) + " 问题子类:" + cell_project_problem_list.loc[j, "fault_subclass"]
                    else:
                        ho_fail_list.loc[i, "src_project_problem_flag"] = ho_fail_list.loc[
                                                                              i, "src_project_problem_flag"] + ";" + "工单编号-" + \
                                                                          str(cell_project_problem_list.loc[
                                                                              j, "order_code"]) + " 问题子类:" + \
                                                                          cell_project_problem_list.loc[j, "fault_subclass"]
                elif ho_fail_list.loc[i, "rel_id2"] == cell_project_problem_list.loc[j, "eci"]:
                    if ho_fail_list.loc[i, "tar_project_problem_flag"] == "":
                        ho_fail_list.loc[i, "tar_project_problem_flag"] = "工单编号-" + str(cell_project_problem_list.loc[
                            j, "order_code"]) + " 问题子类:" + cell_project_problem_list.loc[j, "fault_subclass"]
                    else:
                        ho_fail_list.loc[i, "tar_project_problem_flag"] = ho_fail_list.loc[
                                                                              i, "tar_project_problem_flag"] + ";" + "工单编号-" + \
                                                                          str(cell_project_problem_list.loc[
                                                                              j, "order_code"]) + " 问题子类:" + \
                                                                          cell_project_problem_list.loc[j, "fault_subclass"]

    return ho_fail_list

###pm数据关联
def pm_correlation(ho_fail_list,cell_pm):

    ho_fail_list.loc[:,'ul_busy_prb_use_ratio'] = np.nan
    ho_fail_list.loc[:,'dl_busy_prb_use_ratio'] = np.nan
    ho_fail_list.loc[:,'ul_noise'] = np.nan

    ho_fail_list.loc[:,'n_ul_busy_prb_use_ratio'] = np.nan
    ho_fail_list.loc[:,'n_dl_busy_prb_use_ratio'] = np.nan
    ho_fail_list.loc[:,'n_ul_noise'] = np.nan

    if np.array(cell_pm).shape[0] > 0:
        for i in range(ho_fail_list.shape[0]):
            date = ho_fail_list.loc[i, "date_on_list"]
            date_list_y = date.split(",")
            date_list=[]
            for day in date_list_y:
                if day.endswith('1'):
                    mm = day[0:4] + '-' + day[4:6] + '-' + day[6:8]
                    date_list.append(mm)

            i_cell_pm=cell_pm[cell_pm['pm_date'].isin(date_list)&cell_pm['eci']==ho_fail_list.loc[i,'rel_id']].reset_index(drop=True)
            n_i_cell_pm = cell_pm[cell_pm['pm_date'].isin(date_list) & cell_pm['eci'] == ho_fail_list.loc[i, 'rel_id2']].reset_index(drop=True)
            i_cell_pm['ul_noise'].replace(0, np.nan, inplace=True)
            n_i_cell_pm['ul_noise'].replace(0, np.nan, inplace=True)

            ho_fail_list.loc[i,'ul_busy_prb_use_ratio']=i_cell_pm['ul_busy_prb_use_ratio'].mean() if i_cell_pm['ul_busy_prb_use_ratio'].mean()>0 else np.nan
            ho_fail_list.loc[i, 'dl_busy_prb_use_ratio'] = i_cell_pm['dl_busy_prb_use_ratio'].mean() if i_cell_pm['dl_busy_prb_use_ratio'].mean()>0 else np.nan
            ho_fail_list.loc[i, 'ul_noise'] = i_cell_pm['ul_noise'].mean()

            ho_fail_list.loc[i, 'n_ul_busy_prb_use_ratio'] = n_i_cell_pm['ul_busy_prb_use_ratio'].mean() if n_i_cell_pm[
                                                                                                          'ul_busy_prb_use_ratio'].mean() > 0 else np.nan
            ho_fail_list.loc[i, 'n_dl_busy_prb_use_ratio'] = n_i_cell_pm['dl_busy_prb_use_ratio'].mean() if n_i_cell_pm[
                                                                                                          'dl_busy_prb_use_ratio'].mean() > 0 else np.nan
            ho_fail_list.loc[i, 'n_ul_noise'] = n_i_cell_pm['ul_noise'].mean()

    return ho_fail_list

###mr数据关联
def mr_correlation(ho_fail_list,cell_mr):

    ho_fail_list.loc[:,'overshooting_ratio'] = np.nan
    ho_fail_list.loc[:,'overlap_ratio'] = np.nan
    ho_fail_list.loc[:,'rsrpge110_ratio'] = np.nan

    ho_fail_list.loc[:, 'n_overshooting_ratio'] = np.nan
    ho_fail_list.loc[:, 'n_overlap_ratio'] = np.nan
    ho_fail_list.loc[:, 'n_rsrpge110_ratio'] = np.nan

    if np.array(cell_mr).shape[0] > 0:
        for i in range(ho_fail_list.shape[0]):
            date = ho_fail_list.loc[i, "date_on_list"]
            date_list_y = date.split(",")

            date_list = []
            for day in date_list_y:
                if day.endswith('1'):
                    mm = day[0:4] + '-' + day[4:6] + '-' + day[6:8]
                    date_list.append(mm)


            i_cell_mr = cell_mr[(cell_mr['mr_date'].isin(date_list)) & (cell_mr['eci'] == ho_fail_list.loc[i, 'rel_id'])].reset_index(drop=True)
            n_i_cell_mr = cell_mr[cell_mr['mr_date'].isin(date_list) & cell_mr['eci'] == ho_fail_list.loc[i, 'rel_id2']].reset_index(drop=True)
            #print(i_cell_mr['servingsample'].sum())
            ho_fail_list.loc[i, 'overshooting_ratio'] = i_cell_mr['overshootingsample'].sum()*1.0/i_cell_mr['servingsample'].sum() \
                if i_cell_mr['servingsample'].sum() > 100 else np.nan
            ho_fail_list.loc[i, 'overlap_ratio'] = i_cell_mr['overlap3sample'].sum()*1.0/i_cell_mr['totalsample'].sum() \
                if i_cell_mr['totalsample'].sum() > 100 else np.nan
            ho_fail_list.loc[i, 'rsrpge110_ratio'] = 1-((i_cell_mr['servrsrpdistr0'].sum()+i_cell_mr['servrsrpdistr1'].sum()+
                                                      i_cell_mr['servrsrpdistr2'].sum())*1.0/i_cell_mr['servingsample'].sum()) \
                if i_cell_mr['servingsample'].sum() > 100 else np.nan

            ho_fail_list.loc[i, 'n_overshooting_ratio'] = n_i_cell_mr['overshootingsample'].sum() * 1.0 / n_i_cell_mr[
                'servingsample'].sum() \
                if n_i_cell_mr['servingsample'].sum() > 100 else np.nan
            ho_fail_list.loc[i, 'n_overlap_ratio'] = n_i_cell_mr['overlap3sample'].sum() * 1.0 / n_i_cell_mr['totalsample'].sum() \
                if n_i_cell_mr['totalsample'].sum() > 100 else np.nan
            ho_fail_list.loc[i, 'n_rsrpge110_ratio'] = 1 - ((
                        n_i_cell_mr['servrsrpdistr0'].sum() + n_i_cell_mr['servrsrpdistr1'].sum() +
                        n_i_cell_mr['servrsrpdistr2'].sum()) * 1.0 / n_i_cell_mr['servingsample'].sum()) \
                if n_i_cell_mr['servingsample'].sum() > 100 else np.nan

    return ho_fail_list

###切换类型数据关联
def ho_type(ho_fail_list,siteinfo):

    site = siteinfo.loc[:,["rattype","channel","pci","siteid","cellid","ci","azimuth","hbwd","indoor","height","etilt","mtilt","band","contractor","vendor"]]
    site.rename(columns={'ci':'eci'},inplace=True)
    nsite = siteinfo.loc[:,["rattype","channel","pci","ci","siteid","cellid","azimuth","hbwd","indoor","height","etilt","mtilt","lon","lat","band","contractor","vendor"]]
    nsite.rename(
        columns={'rattype': 'n_rattype', 'channel': 'n_channel', 'ci':'n_eci','pci': 'n_pci', 'siteid': 'n_siteid',
                 'cellid':'n_cellid','azimuth':'n_azimuth','hbwd':'n_hbwd','indoor':'n_indoor','height':'n_height'
                ,'etilt':'n_etilt','mtilt':'n_mtilt','lon':'n_lon','lat':'n_lat','band':'n_band',"contractor":"n_contractor","vendor":"n_vendor"},inplace=True)

    #site["eci"] = site[["siteid","cellid"]].apply(lambda x:x["siteid"]*256+x["cellid"],axis=1)
    #nsite["n_eci"] = nsite[["n_siteid","n_cellid"]].apply(lambda x:x["n_siteid"]*256+x["n_cellid"],axis=1)

    ho_fail_list=pd.merge(ho_fail_list,site,left_on='rel_id',right_on='eci',how='left')
    ho_fail_list=pd.merge(ho_fail_list,nsite,left_on='rel_id2',right_on='n_eci',how='left')

    for i in range(ho_fail_list.shape[0]):
        ##判断同系统异系统
        if not pd.isnull(ho_fail_list.loc[i,'n_rattype']):
            if ho_fail_list.loc[i,'n_rattype']!='4G':
                ho_fail_list.loc[i,'irat_flag']="异系统切换"
                ho_fail_list.loc[i, 'intrafrequency_flag'] = "-"
            else:
                ho_fail_list.loc[i, 'irat_flag'] = "系统内切换"
                if (not pd.isnull(ho_fail_list.loc[i,'channel'])) and  (not pd.isnull(ho_fail_list.loc[i,'n_channel'])):
                    if ho_fail_list.loc[i,'channel'] != ho_fail_list.loc[i,'n_channel']:
                        ho_fail_list.loc[i, 'intrafrequency_flag'] = "异频切换"
                    else:
                        ho_fail_list.loc[i, 'intrafrequency_flag'] = "同频切换"
                else:
                    ho_fail_list.loc[i, 'intrafrequency_flag'] = "-"
        else:
            ho_fail_list.loc[i, 'irat_flag'] = "-"
            ho_fail_list.loc[i, 'intrafrequency_flag'] = "-"

        ##判断室内室外
        if (not pd.isnull(ho_fail_list.loc[i,'indoor'])) and (not pd.isnull(ho_fail_list.loc[i,'n_indoor'])) :
            if ho_fail_list.loc[i,'indoor']==1 and ho_fail_list.loc[i,'n_indoor']==1:
                ho_fail_list.loc[i, 'indoor_outdoor_ho_type'] = "室内小区->室内小区"
            elif ho_fail_list.loc[i,'indoor']==0 and ho_fail_list.loc[i,'n_indoor']==0:
                ho_fail_list.loc[i, 'indoor_outdoor_ho_type'] = "室外小区->室外小区"
            elif ho_fail_list.loc[i,'indoor']==1 and ho_fail_list.loc[i,'n_indoor']==0:
                ho_fail_list.loc[i, 'indoor_outdoor_ho_type'] = "室内小区->室外小区"
            elif ho_fail_list.loc[i,'indoor']==0 and ho_fail_list.loc[i,'n_indoor']==1:
                ho_fail_list.loc[i, 'indoor_outdoor_ho_type'] = "室外小区->室内小区"
            else:
                ho_fail_list.loc[i, 'indoor_outdoor_ho_type'] = "-"
        else:
            ho_fail_list.loc[i, 'indoor_outdoor_ho_type'] = "-"
        ##判断厂家
        if (not pd.isnull(ho_fail_list.loc[i,'vendor'])) and (not pd.isnull(ho_fail_list.loc[i,'n_vendor'])) :
            if ho_fail_list.loc[i,'vendor']!=ho_fail_list.loc[i,'n_vendor']:
                ho_fail_list.loc[i, 'ho_vendor_type'] = "异厂家_"+ho_fail_list.loc[i,'vendor']+"->"+ho_fail_list.loc[i,'n_vendor']
            else:
                ho_fail_list.loc[i, 'ho_vendor_type'] = "同厂家_"+ho_fail_list.loc[i,'vendor']+"->"+ho_fail_list.loc[i,'n_vendor']
        else:
            ho_fail_list.loc[i, 'ho_vendor_type'] = "-"

        ##判断承建方
        if (not pd.isnull(ho_fail_list.loc[i, 'contractor'])) and (
        not pd.isnull(ho_fail_list.loc[i, 'n_contractor'])):
            if ho_fail_list.loc[i, 'contractor'] != ho_fail_list.loc[i, 'n_contractor']:
                ho_fail_list.loc[i, 'ho_contractor_type'] = "不同运营商_"+ho_fail_list.loc[i, 'contractor']+"->"+ho_fail_list.loc[i, 'n_contractor']
            else:
                ho_fail_list.loc[i, 'ho_contractor_type'] = "相同运营商_"+ho_fail_list.loc[i, 'contractor']+"->"+ho_fail_list.loc[i, 'n_contractor']
        else:
            ho_fail_list.loc[i, 'ho_contractor_type'] = "-"


            ###距离计算
        if (not pd.isnull(ho_fail_list.loc[i,"lon"])) and (not pd.isnull(ho_fail_list.loc[i,"lat"])) and \
                (not pd.isnull(ho_fail_list.loc[i,"n_lon"])) and (not pd.isnull(ho_fail_list.loc[i,"n_lat"])):
            dis_x= calc_greate_circle_distance([ho_fail_list.loc[i,"lon"],ho_fail_list.loc[i,"lat"]],
                                                                                     [ho_fail_list.loc[i, "n_lon"],ho_fail_list.loc[i, "n_lat"]])
            ho_fail_list.loc[i, "dis_srcell_to_tarcell"]='-' if (math.isnan(dis_x)) else dis_x
        else:
            ho_fail_list.loc[i, "dis_srcell_to_tarcell"] = '-'


    return ho_fail_list

###告警数据关联
def alarm_correlation(ho_fail_list,cell_alarm):
    ho_fail_list.loc[:,"ho_fail_alarm_day_num"] = 0
    ho_fail_list.loc[:,"ho_fail_alarm_day"] = '0'
    ho_fail_list.loc[:,"ho_fail_alarm_num"] = '0'
    ho_fail_list.loc[:,"ho_fail_top1_alarm_name"] = '0'

    ho_fail_list.loc[:,"n_ho_fail_alarm_day_num"] = 0
    ho_fail_list.loc[:,"n_ho_fail_alarm_day"] = '0'
    ho_fail_list.loc[:,"n_ho_fail_alarm_num"] = '0'
    ho_fail_list.loc[:,"n_ho_fail_top1_alarm_name"] = '0'

    if np.array(cell_alarm).shape[0]>0:
        for i in range(ho_fail_list.shape[0]):
            alarm_temp = cell_alarm[cell_alarm["eci"] == ho_fail_list.loc[i, "rel_id"]].reset_index(drop=True)
            n_alarm_temp = cell_alarm[cell_alarm["eci"] == ho_fail_list.loc[i, "rel_id2"]].reset_index(drop=True)
            if alarm_temp.empty:
                pass
            else:
                ##判断切换失败当日是否有告警
                date = ho_fail_list.loc[i,"date_on_list"]
                date_list = date.split(",")
                for day in date_list:

                    if day.endswith('1'):  ###当日数据异常
                        mm = day[0:4] + '-' + day[4:6] + '-' + day[6:8]
                        for n in range(alarm_temp.shape[0]):

                            if alarm_temp.loc[n,'alarm_date']==mm:
                                ho_fail_list.loc[i,"ho_fail_alarm_day_num"] = ho_fail_list.loc[i,"ho_fail_alarm_day_num"]+1
                                if ho_fail_list.loc[i,"ho_fail_alarm_day_num"]==1:
                                    ho_fail_list.loc[i, "ho_fail_alarm_day"] = mm
                                    ho_fail_list.loc[i, "ho_fail_alarm_num"] = str(
                                        int(alarm_temp.loc[n, 'important_alarm_num']) + int(
                                            alarm_temp.loc[n, 'urgent_alarm_num']))
                                    ho_fail_list.loc[i, "ho_fail_top1_alarm_name"] = alarm_temp.loc[n, 'top1_alarm_name']
                                else:
                                    ho_fail_list.loc[i, "ho_fail_alarm_day"] = ho_fail_list.loc[
                                                                                   i, "ho_fail_alarm_day"] + ',' + mm
                                    ho_fail_list.loc[i, "ho_fail_alarm_num"] = ho_fail_list.loc[
                                                                                   i, "ho_fail_alarm_num"] + ',' + str(
                                        int(alarm_temp.loc[n, 'important_alarm_num']) + int(
                                            alarm_temp.loc[n, 'urgent_alarm_num']))
                                    ho_fail_list.loc[i, "ho_fail_top1_alarm_name"] = ho_fail_list.loc[
                                                                                         i, "ho_fail_top1_alarm_name"] + ',' + \
                                                                                     alarm_temp.loc[n, 'top1_alarm_name']
                            break
            if n_alarm_temp.empty:
                pass
            else:
                ##判断切换失败当日是否有告警
                date = ho_fail_list.loc[i, "date_on_list"]
                date_list = date.split(",")
                for day in date_list:
                    if day.endswith('1'):  ###当日数据异常
                        mm = day[0:4] + '-' + day[4:6] + '-' + day[6:8]

                        for n in range(n_alarm_temp.shape[0]):
                            if n_alarm_temp.loc[n, 'alarm_date'] == mm:
                                ho_fail_list.loc[i, "n_ho_fail_alarm_day_num"] = ho_fail_list.loc[
                                                                                     i, "n_ho_fail_alarm_day_num"] + 1
                                if ho_fail_list.loc[i,"n_ho_fail_alarm_day_num"]==1:
                                    ho_fail_list.loc[i, "n_ho_fail_alarm_day"] = mm
                                    ho_fail_list.loc[i, "n_ho_fail_alarm_num"] = str(
                                        int(n_alarm_temp.loc[n, 'important_alarm_num']) + int(
                                            n_alarm_temp.loc[n, 'urgent_alarm_num']))
                                    ho_fail_list.loc[i, "n_ho_fail_top1_alarm_name"] = n_alarm_temp.loc[n, 'top1_alarm_name']
                                else:
                                    ho_fail_list.loc[i, "n_ho_fail_alarm_day_num"] = ho_fail_list.loc[
                                                                                         i, "n_ho_fail_alarm_day_num"] + 1
                                    ho_fail_list.loc[i, "n_ho_fail_alarm_day"] = ho_fail_list.loc[
                                                                                     i, "n_ho_fail_alarm_day"] + ',' + mm
                                    ho_fail_list.loc[i, "n_ho_fail_alarm_num"] = ho_fail_list.loc[
                                                                                     i, "n_ho_fail_alarm_num"] + ',' + str(
                                        int(n_alarm_temp.loc[n, 'important_alarm_num']) + int(
                                            n_alarm_temp.loc[n, 'urgent_alarm_num']))
                                    ho_fail_list.loc[i, "n_ho_fail_top1_alarm_name"] = ho_fail_list.loc[
                                                                                           i, "n_ho_fail_top1_alarm_name"] + ',' + \
                                                                                       n_alarm_temp.loc[
                                                                                           n, 'top1_alarm_name']
                            break
    return ho_fail_list


def get_last_week(date=None):
    if date:
        today = datetime.datetime.strptime(str(date), '%Y-%m-%d')
    else:
        today = datetime.datetime.today()
    end_time = today - datetime.timedelta(days=today.isoweekday())
    start_time = end_time - datetime.timedelta(days=6)
    return start_time.strftime("%Y-%m-%d"), end_time.strftime("%Y-%m-%d")


start_day,end_day = get_last_week()

ho_fail_list, cell_pm, cell_mr, cell_project_problem_list, cell_alarm, siteinfo = db_read(start_day,end_day,'高切换失败小区对')

ho_fail_list=ho_type(ho_fail_list,siteinfo)

ho_fail_list=alarm_correlation(ho_fail_list,cell_alarm)

ho_fail_list=project_problem_correlation(ho_fail_list,cell_project_problem_list)

ho_fail_list=pm_correlation(ho_fail_list,cell_pm)

ho_fail_list=mr_correlation(ho_fail_list,cell_mr)

ho_fail_list=ho_diagnosis(ho_fail_list)

update_workorder_cellpair=ho_fail_list.loc[:,['root_cause','tuning_action','comments','order_sorting','order_code']]

update_CP=update_workorder_cellpair.apply(lambda x: tuple(x), axis=1).tolist()

conn = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502",
                        host="133.160.191.111", port="5432")
print("Postgres Connection success-X")
cur = conn.cursor()

sql="update cnio.fault_workorder set root_cause = %s,tuning_action = %s,comments = %s, \
    order_sorting = %s where order_code= %s;"

#print(tuple(ho_fail_list['comments']))
batch_update=cur.executemany(sql,update_CP)

conn.commit()
cur.close()
conn.close()
print("Postgres Release success-X")










