import pandas as pd
import psycopg2
import datetime as dt
from io import StringIO
import math
import multiprocessing as mp
import time


########################
# 开发人员：qiaojj
# 代码功能：基于lte_conn、lte_mr_conn、uuextend_userconn构建诊断表单，并关联告警、pm等数据，完成单用户诊断
# 修改时间：2022-11-18
# #######################
def get_date():
    today = dt.datetime.today()
    yestoday = today + dt.timedelta(days=(-1))

    yestoday_time = yestoday.strftime('%Y%m%d')
    yestoday_time_v = yestoday.strftime('%Y-%m-%d')
    year = yestoday.strftime('%Y')

    delta_time_ms = (dt.datetime(yestoday.year, yestoday.month, yestoday.day) - dt.datetime(2022, 6,
                                                                                            15)).days * 86400 * 1000

    return yestoday_time, yestoday_time_v, year, delta_time_ms

def get_analysis_data(yestoday, year,expert_db_account):
    conn = psycopg2.connect(database=expert_db_account["database"], user=expert_db_account["user"],
                            password=expert_db_account["password"],host=expert_db_account["host"],
                            port=expert_db_account["port"])
    print("exppert database Connection success")

    cur = conn.cursor()
    # 获取pm数据
    sql_pm = """
    select
        (oid*256+cellid) as eci,
        (case
            when KPI_0000000614 = 0 then -1
            else KPI_0000000612 / KPI_0000000614
        end )as busy_ul_prb_use_ratio,
        (case
            when KPI_0000000615 = 0 then -1
            else KPI_0000000613 / KPI_0000000615
        end) as busy_dl_prb_use_ratio,
        KPI_0000000655 as idle_prb_noise_poser
        
    from
        searching.pm_4g_zdy_day s
    where
        daytime = 20221022
    """
    cur.execute(sql_pm)
    pm_hour_data = cur.fetchall()
    if len(pm_hour_data) != 0:
        pm_hour_data_pd = pd.DataFrame(pm_hour_data)
        des_pm = cur.description
        colname = []
        for item in des_pm:
            colname.append(item[0])
        pm_hour_data_pd.columns = colname
    else:
        pm_hour_data_pd = pm_hour_data

    # 获取告警数据
    sql_pm = """
    select
        eci,
        top1_alarm_num ,
        top1_alarm_name
    from
        searching.alarm_hz_day
    where
        data_time = '2022-10-20'
    """
    cur.execute(sql_pm)
    alarm_day_data = cur.fetchall()
    if len(alarm_day_data) != 0:
        alarm_day_data_pd = pd.DataFrame(alarm_day_data)
        des_alarm = cur.description
        colname = []
        for item in des_alarm:
            colname.append(item[0])
        alarm_day_data_pd.columns = colname
    else:
        alarm_day_data_pd = alarm_day_data

    # 获取规划站数据
    sql_plan = """
    select  station_name,lon,lat
      from cnio.dwa_plan_site_detail dpsd where network_system ='4G' and to_char(datetime, 'yyyy')='%s' and indoor_or_outdoor ='宏站'
    """ % year
    cur.execute(sql_plan)
    plansite_data = cur.fetchall()
    if len(plansite_data) != 0:
        plansite_data_pd = pd.DataFrame(plansite_data)
        des_alarm = cur.description
        colname = []
        for item in des_alarm:
            colname.append(item[0])
        plansite_data_pd.columns = colname
    else:
        plansite_data_pd = plansite_data

    # 获取top基站
    sql_site_structure = """
    select  siteid,top1_site_id ,top1_site_distance*1000 as top1_site_distance,distance*1000 as sitedistance  from cnio.lte_site_structure lss  where data_date = (select max(data_date) from cnio.lte_site_structure lss2) 
    """
    cur.execute(sql_site_structure)
    top1_site_data = cur.fetchall()
    if len(top1_site_data) != 0:
        top1_site_data_pd = pd.DataFrame(top1_site_data)
        des_alarm = cur.description
        colname = []
        for item in des_alarm:
            colname.append(item[0])
        top1_site_data_pd.columns = colname
    else:
        top1_site_data_pd = top1_site_data

    # 获取服务小区经纬度
    sql_siteinfo = """
    select
        ci as eci,
        indoor,
        lon as celllon ,
        lat as celllat,
        height,
        azimuth,
        (mtilt + etilt) as tilt,
        cellname_z as cellname,
        channel 
    from
        cnio.siteinfo s
    where
        rattype = '4G'
        and data_date = (
        select
            max(data_date)
        from
            cnio.siteinfo s2)    
		"""
    cur.execute(sql_siteinfo)
    siteinfo_data = cur.fetchall()
    if len(siteinfo_data) != 0:
        siteinfo_data_pd = pd.DataFrame(siteinfo_data)
        des_siteinfo = cur.description
        colname = []
        for item in des_siteinfo:
            colname.append(item[0])
        siteinfo_data_pd.columns = colname
    else:
        siteinfo_data_pd = siteinfo_data
    return pm_hour_data_pd, alarm_day_data_pd, plansite_data_pd, top1_site_data_pd, siteinfo_data_pd

def get_call_recorder(yestoday, yestoday_v,trace_db_account):
    conn = psycopg2.connect(database=trace_db_account["database"], user=trace_db_account["user"],
                            password=trace_db_account["password"],host=trace_db_account["host"],
                            port=trace_db_account["port"])
    print("Trace Postgres Connection success")

    cur = conn.cursor()
    # 获取taskid
    sql_trace_sk_task = """
    select taskid from public.sk_task where file_filter =%s and status='完成'
    """
    cur.execute(sql_trace_sk_task, [yestoday])
    taskid = cur.fetchall()

    data = []

    taskid = [2]  # 测试
    ## 如果taskid不为空，
    if len(taskid) == 0:
        pass
    else:
        # taskid = pd.DataFrame(taskid)
        #
        # ##测试
        # taskid.columns = ["taskid"]
        #
        # id = int(taskid.loc[0, "taskid"])
        id = 2
        sql = """
                select
            *
        from
            (
            select
                g.*,
                1 as taskid,
                h.servingsample as tar_servingsample,
                h.servingrsrp as tar_servingrsrp,
                ( h.servrsrpdistr0 + h.servrsrpdistr1 + h.servrsrpdistr2 ) as tar_rsrp_Ge110_sample,
                ( h.servrsrpdistr0 + h.servrsrpdistr1 ) as tar_rsrp_Ge115_sample,
                ( h.servrsrpdistr0 + h.servrsrpdistr1 + h.servrsrpdistr2 + h.servrsrpdistr3 ) as tar_rsrp_Ge105_sample,
                h.overlap3sample as tar_overlap3sample,
                h.overlap4sample as tar_overlap4sample,
                h.overshootingsample as tar_overshootingsample,
                h.poorcoveragesample as tar_poorcoveragesample,
                h.mod3sample as tar_mod3sample
            from
                (
                select
                    e.*,
                    f.servingsample as src_servingsample,
                    f.servingrsrp as src_servingrsrp,
                    ( f.servrsrpdistr0 + f.servrsrpdistr1 + f.servrsrpdistr2 ) as src_rsrp_Ge110_sample,
                    ( f.servrsrpdistr0 + f.servrsrpdistr1 ) as src_rsrp_Ge115_sample,
                    ( f.servrsrpdistr0 + f.servrsrpdistr1 + f.servrsrpdistr2 + f.servrsrpdistr3 ) as src_rsrp_Ge105_sample,
                    f.overlap3sample as src_overlap3sample,
                    f.overlap4sample as src_overlap4sample,
                    f.overshootingsample as src_overshootingsample,
                    f.poorcoveragesample as src_poorcoveragesample,
                    f.mod3sample as src_mod3sample
                from
                    (
                    select
                        c.*,
                        d.ul_sinr_avg,
                        d.ul_sinr_num,
                        d.ul_sinrdistr0,
                        d.ul_sinrdistr1,
                        d.ul_sinrdistr2,
                        d.ul_sinrdistr3,
                        d.ul_sinrdistr4,
                        d.ul_sinrdistr5,
                        d.ul_sinrdistr6,
                        d.ul_sinrdistr7,
                        d.rip_avg,
                        d.rip_num,
                        d.ul_user_plane_bytes,
                        d.dl_user_plane_bytes,
                        d.ul_prb_drb,
                        d.dl_prb_drb,
                        d.dl_prb,
                        d.ul_prb,
                        d.ul_prb_total,
                        d.dl_prb_total,
                        d.dl_pdcp_sdu_packs_lost_drb,
                        d.dl_pdcp_sdu_packs_discard_drb,
                        d.dl_pdcp_sdu_packs_tx_drb,
                        d.dl_pdcp_bytes,
                        d.ul_pdcp_bytes,
                        d.ul_pdcp_sdu_packs_lost_drb,
                        d.ul_pdcp_sdu_packs_expected_drb,
                        d.ul_tbs_qpsk,
                        d.ul_tbs_16qam,
                        d.ul_tbs_64qam,
                        d.dl_tbs_qpsk,
                        d.dl_tbs_16qam,
                        d.dl_tbs_64qam,
                        d.cqinum,
                        d.cqidistr0,
                        d.cqidistr1,
                        d.cqidistr2,
                        d.rinum,
                        d.ri2num
                    from
                        (
                        select
                            a.*,
                            b.earfcn,
                            b.pci,
                            b.servrsrp,
                            b.totalsample,
                            b.servnotbestsample,
                            b.poorcoveragesample,
                            b.overlap3sample,
                            b.overlap4sample,
                            b.overshootingsample,
                            b.servdeltadistr0,
                            b.servdeltadistr1,
                            b.servdeltadistr2,
                            b.servdeltadistr3,
                            b.servdeltadistr4,
                            b.servdeltadistr5,
                            b.servdeltadistr6,
                            b.servdeltadistr7,
                            b.nb1pci,
                            b.nb1rsrp,
                            b.nb1deltarsrp,
                            b.nb1sample,
                            b.nb2pci,
                            b.nb2rsrp,
                            b.nb2deltarsrp,
                            b.nb2sample,
                            b.nb3pci,
                            b.nb3rsrp,
                            b.nb3deltarsrp,
                            b.nb3sample,
                            b.interfreq,
                            b.interfreqnbpci,
                            b.interfreqnbrsrp
                        from
                            (
                            select
                                connid,
                                callid,
                                servicetype,
                                connesttype,
                                connstatus,
                                endcause,
                                starttime,
                                endtime,
                                lon,
                                lat,
                                gridx,
                                gridy,
                                mmeuserid,
                                srcmmeuserid,
                                enbues1apid,
                                msisdn,
                                m_tmsi,
                                crnti,
                                enbid,
                                cellid,
                                destenbid,
                                destcellid,
                                uecapnr,
                                redirrat,
                                hodelay,
                                hoouttotalnum,
                                hooutfailnum,
                                fileid
                            from
                                task_%s_lte_conn tb
                            where
                                ( (connstatus=2 or connstatus=3) and endcause!=144
                                     )
                                and (endtime-starttime)>10000 --and lon>0 and lat>0  
                        )
                        a
                        left join task_%s_lte_mr_userconn b on
                            a.connid = b.connid 
                    )
                    c
                    left join task_%s_lte_uuextend_userconn d on
                        c.connid = d.connid 
                ) e
                left join task_%s_lte_mr_cell_total f on
                    e.enbid * 256 + e.cellid = f.eci 
            )
            g
            left join task_%s_lte_mr_cell_total h on
                g.enbid * 256 + g.cellid = h.eci) x

        """
        cur.execute(sql, [id, id, id, id, id])

        data = cur.fetchall()
        if len(data) == 0:
            pass
        else:
            data = pd.DataFrame(data)
            des = cur.description
            colname = []
            for item in des:
                colname.append(item[0])
            data.columns = colname

    print("Trace Postgres Connection close")
    print("data get: success")

    return data


def expert_create_talbe(yestoday,expert_db_account):
    conn = psycopg2.connect(database=expert_db_account["database"], user=expert_db_account["user"],
                            password=expert_db_account["password"],host=expert_db_account["host"],
                            port=expert_db_account["port"])

    print("Postgres Connection success")
    cur = conn.cursor()

    sql_drop_table = "drop table if exists cnio.lte_abnormal_call_detail_" + yestoday
    sql_create_table = """
        create table cnio.lte_abnormal_call_detail_%s(
        connid	int8	,
        callid	int8	,
        servicetype	int4	,
        connesttype	int4	,
        connstatus	int4	,
        endcause	int4	,
        starttime	int8	,
        endtime	int8	,
        lon	int4	,
        lat	int4	,
        gridx	int4	,
        gridy	int4	,
        mmeuserid	int8	,
        srcmmeuserid	int8	,
        enbues1apid	int8	,
        msisdn	int8	,
        m_tmsi	int8	,
        crnti	int4	,
        enbid	int4	,
        cellid	int4	,
        destenbid	int4	,
        destcellid	int4	,
        uecapnr	int4	,
        redirrat	int4	,
        hodelay	int4	,
        hoouttotalnum	int4	,
        hooutfailnum	int4	,
        fileid	int8	,
        earfcn	float4	,
        pci	int4	,
        servrsrp	float4	,
        totalsample	int4	,
        servnotbestsample	int4	,
        poorcoveragesample	int4	,
        overlap3sample	int4	,
        overlap4sample	int4	,
        overshootingsample	int4	,
        servdeltadistr0	int4	,
        servdeltadistr1	int4	,
        servdeltadistr2	int4	,
        servdeltadistr3	int4	,
        servdeltadistr4	int4	,
        servdeltadistr5	int4	,
        servdeltadistr6	int4	,
        servdeltadistr7	int4	,
        nb1pci	int4	,
        nb1rsrp	float4	,
        nb1deltarsrp	float4	,
        nb1sample	int4	,
        nb2pci	int4	,
        nb2rsrp	float4	,
        nb2deltarsrp	float4	,
        nb2sample	int4	,
        nb3pci	int4	,
        nb3rsrp	float4	,
        nb3deltarsrp	float4	,
        nb3sample	int4	,
        interfreq	int4	,
        interfreqnbpci	int4	,
        interfreqnbrsrp	float4	,
        ul_sinr_avg	float4	,
        ul_sinr_num	float4	,
        ul_sinrdistr0	float4	,
        ul_sinrdistr1	float4	,
        ul_sinrdistr2	float4	,
        ul_sinrdistr3	float4	,
        ul_sinrdistr4	float4	,
        ul_sinrdistr5	float4	,
        ul_sinrdistr6	float4	,
        ul_sinrdistr7	float4	,
        rip_avg	float8	,
        rip_num	float8	,
        ul_user_plane_bytes	float8	,
        dl_user_plane_bytes	float8	,
        ul_prb_drb	float8	,
        dl_prb_drb	float8	,
        dl_prb	float8	,
        ul_prb	float8	,
        ul_prb_total	float8	,
        dl_prb_total	float8	,
        dl_pdcp_sdu_packs_lost_drb	float8	,
        dl_pdcp_sdu_packs_discard_drb	float8	,
        dl_pdcp_sdu_packs_tx_drb	float8	,
        dl_pdcp_bytes	float8	,
        ul_pdcp_bytes	float8	,
        ul_pdcp_sdu_packs_lost_drb	float8	,
        ul_pdcp_sdu_packs_expected_drb	float8	,
        ul_tbs_qpsk	float8	,
        ul_tbs_16qam	float8	,
        ul_tbs_64qam	float8	,
        dl_tbs_qpsk	float8	,
        dl_tbs_16qam	float8	,
        dl_tbs_64qam	float8	,
        cqinum	float8	,
        cqidistr0	float8	,
        cqidistr1	float8	,
        cqidistr2	float8	,
        rinum	float8	,
        ri2num	float8	,
        src_servingsample	float8	,
        src_servingrsrp	float8	,
        src_rsrp_ge110_sample	float8	,
        src_rsrp_ge115_sample	float8	,
        src_rsrp_ge105_sample	float8	,
        src_overlap3sample	float8	,
        src_overlap4sample	float8	,
        src_overshootingsample	float8	,
        src_poorcoveragesample	float8	,
        src_mod3sample	float8	,
        taskid	int4	,
        tar_servingsample	float8	,
        tar_servingrsrp	float8	,
        tar_rsrp_ge110_sample	float8	,
        tar_rsrp_ge115_sample	float8	,
        tar_rsrp_ge105_sample	float8	,
        tar_overlap3sample	float8	,
        tar_overlap4sample	float8	,
        tar_overshootingsample	float8	,
        tar_poorcoveragesample	float8	,
        tar_mod3sample	float8	,
        overlap_ratio	float8	,
        tar_overshooting_ratio	float8	,
        eci	int8	,
        tar_eci	int8	,
        indoor	float4	,
        celllon	float4	,
        celllat	float4	,
        height	float4	,
        azimuth	float4	,
        tilt	float4	,
        cellname text,
        channel float4,
        distance	float4	,
        tar_indoor	float4	,
        tar_celllon	float4	,
        tar_celllat	float4	,
        tar_height	float4	,
        tar_azimuth	float4	,
        tar_tilt	float4	,
        tar_cellname text,
        tar_channel float4,
        tar_distance	float4	,
        busy_ul_prb_use_ratio	float4	,
        busy_dl_prb_use_ratio	float4	,
        idle_prb_noise_poser	float4	,
        tar_busy_ul_prb_use_ratio	float4	,
        tar_busy_dl_prb_use_ratio	float4	,
        tar_idle_prb_noise_poser	float4	,
        top1_alarm_num	float4	,
        top1_alarm_name	text	,
        tar_top1_alarm_num	float4	,
        tar_top1_alarm_name	text	,
        pci_confuse_flag 	float4	,
        pci_confuse_eci 	text	,
        neighbour_mismatch_flag	float4	,
        neighbour_mismatch_eci	text	,
        plansite_flag	float4	,
        tar_plansite_flag	float4	,
        siteid	float4	,
        top1_site_id	float4	,
        top1_site_distance	float4	,
        sitedistance float4,
        tar_siteid	float4	,
        tar_top1_site_id	float4	,
        tar_top1_site_distance	float4	,
        tar_sitedistance float4,
        user_top1_site_id float4,
        user_top1_site_distance float4,
        description text,
        classify 	text	,
        root_cause 	text	,
        action 	text	


        );
    """ % yestoday
    cur.execute(sql_drop_table)
    conn.commit()
    cur.execute(sql_create_table)
    conn.commit()
    cur.close()
    conn.close()
    print("create tablee: success")


def data_load(data, table,expert_db_account):
    output = StringIO()
    data.to_csv(output, sep=f'\&', index=False, header=False)
    output1 = output.getvalue()
    conn = psycopg2.connect(host=expert_db_account["host"], user=expert_db_account["user"],
                            password=expert_db_account["password"], database=expert_db_account["database"],
                            port=expert_db_account["port"], options=expert_db_account["options"])
    cur = conn.cursor()
    cur.copy_from(StringIO(output1), table, sep=f'\$', null='')
    conn.commit()
    cur.close()
    conn.close()
    print("abnormal_call sync: success")


def get_distance(lola1, lola2):
    lat1 = lola1[1]
    lon1 = lola1[0]
    lat2 = lola2[1]
    lon2 = lola2[0]
    R = 6371000
    lat1_rad = lat1 * math.pi / 180
    lon1_rad = lon1 * math.pi / 180
    lat2_rad = lat2 * math.pi / 180
    lon2_rad = lon2 * math.pi / 180
    d = math.sqrt(math.pow(math.cos((lat1_rad + lat2_rad) / 2) * R * (lon1_rad - lon2_rad), 2) +
                  math.pow(R * (lat1_rad - lat2_rad), 2))
    if d < 10000:
        return float(d)
    else:
        delta_lat = lat2_rad - lat1_rad
        delta_lon = lon2_rad - lon1_rad
        a = math.sin(delta_lat / 2) * math.sin(delta_lat / 2) + \
            math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) * math.sin(delta_lon / 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return float(R * c)


def single_user_call_daignosis(trace_data, pm_data, alarm_data, plansite_data, top1_site_data, siteinfo_data):
    # 重叠覆盖率计算
    trace_data["overlap_ratio"] = trace_data[["overlap3sample", "totalsample"]].apply(
        lambda x: x["overlap3sample"] / x["totalsample"] if x["totalsample"] > 0 else -1, axis=1)
    # 越区采样点比例计算
    trace_data["tar_overshooting_ratio"] = trace_data[
        ["tar_overshootingsample", "tar_servingsample"]].apply(
        lambda x: x["tar_overshootingsample"] / x["tar_servingsample"]
        if x["tar_servingsample"] > 0 else -1, axis=1)

    # 添加关联字段 eci tar_eci
    trace_data["eci"] = trace_data[["enbid", "cellid"]].apply(lambda x: x["enbid"] * 256 + x["cellid"], axis=1)
    trace_data["tar_eci"] = trace_data[["destenbid", "destcellid"]].apply(
        lambda x: x["destenbid"] * 256 + x["destcellid"], axis=1)

    if len(siteinfo_data) != 0:
        trace_data = pd.merge(trace_data, siteinfo_data, on=["eci"], how="left")
        trace_data["distance"] = trace_data[["celllon", "celllat", "lon", "lat"]].apply(
            lambda x: get_distance([x[0], x[1]], [x[2] / 100000.0, x[3] / 100000.0]) if x[2]!=0 else None, axis=1)

        tar_siteinfo_data = siteinfo_data.copy()
        tar_siteinfo_data.rename(columns={"eci": "tar_eci", "indoor": "tar_indoor",
                                          "celllon": "tar_celllon", "celllat": "tar_celllat",
                                          "height": "tar_height", "azimuth": "tar_azimuth", "tilt": "tar_tilt",
                                          "cellname": "tar_cellname", "channel": "tar_channel"
                                          }, inplace=True)
        trace_data = pd.merge(trace_data, tar_siteinfo_data, on=["tar_eci"], how="left")
        trace_data["tar_distance"] = trace_data[["tar_celllon", "tar_celllat", "lon", "lat"]].apply(
            lambda x: get_distance([x[0], x[1]], [x[2] / 100000.0, x[3] / 100000.0]  )  if x[2]!=0 else None , axis=1)

    else:
        trace_data["indoor"] = None
        trace_data["celllon"] = None
        trace_data["celllat"] = None
        trace_data["height"] = None
        trace_data["azimuth"] = None
        trace_data["tilt"] = None
        trace_data["cellname"] = None
        trace_data["channel"] = None
        trace_data["distance"] = None

        trace_data["tar_indoor"] = None
        trace_data["tar_celllon"] = None
        trace_data["tar_celllat"] = None
        trace_data["tar_height"] = None
        trace_data["tar_azimuth"] = None
        trace_data["tar_tilt"] = None
        trace_data["tar_cellname"] = None
        trace_data["tar_channel"] = None
        trace_data["tar_distance"] = None

    # pm数据关联
    if len(pm_data) != 0:
        # 源小区 pm数据关联
        trace_data = pd.merge(trace_data, pm_data, on=["eci"], how="left")
        # 目标小区 pm数据关联
        tar_pm_data = pm_data.copy()
        tar_pm_data.rename(columns={"eci": "tar_eci", "idle_prb_noise_poser": "tar_idle_prb_noise_poser",
                                    "busy_ul_prb_use_ratio": "tar_busy_ul_prb_use_ratio",
                                    "busy_dl_prb_use_ratio": "tar_busy_dl_prb_use_ratio"
                                    }, inplace=True)
        trace_data = pd.merge(trace_data, tar_pm_data, on=["tar_eci"], how="left")
    else:
        trace_data["idle_prb_noise_poser"] = None
        trace_data["busy_ul_prb_use_ratio"] = None
        trace_data["busy_dl_prb_use_ratio"] = None
        trace_data["tar_idle_prb_noise_poser"] = None
        trace_data["tar_busy_ul_prb_use_ratio"] = None
        trace_data["tar_busy_dl_prb_use_ratio"] = None

    ## 告警数据关联
    if len(alarm_data) != 0:
        # 源小区告警关联
        trace_data = pd.merge(trace_data, alarm_data, on=["eci"], how="left")
        # 目标小区告警关联
        tar_alarm_data = alarm_data.copy()
        tar_alarm_data.rename(columns={"eci": "tar_eci", "top1_alarm_num": "tar_top1_alarm_num",
                                       "top1_alarm_name": "tar_top1_alarm_name"}, inplace=True)
        trace_data = pd.merge(trace_data, tar_alarm_data, on=["tar_eci"], how="left")
    else:
        trace_data["top1_alarm_num"] = None
        trace_data["top1_alarm_name"] = None
        trace_data["tar_top1_alarm_num"] = None
        trace_data["tar_top1_alarm_name"] = None

    trace_data["pci_confuse_flag"] = None
    trace_data["pci_confuse_eci"] = None

    trace_data["neighbour_mismatch_flag"] = None
    trace_data["neighbour_mismatch_eci"] = None

    # 规划站关联计算
    # if len(plansite_data)!=0:
    #     # 源小区告警关联
    #     trace_data = pd.merge(trace_data, plansite_data, on=["eci"], how="left")
    #     # 目标小区告警关联
    #     tar_alarm_data = alarm_data.copy()
    #     tar_alarm_data.rename(columns={"eci": "tar_eci","alarm_totalnum": "tar_alarm_totalnum"}, inplace=True)
    #     trace_data = pd.merge(trace_data, tar_alarm_data, on=["tar_eci"], how="left")
    # else:
    trace_data["plansite_flag"] = None
    trace_data["tar_plansite_flag"] = None

    if len(top1_site_data) != 0:
        # 源小区告警关联
        top1_site_data = top1_site_data.drop_duplicates(subset='siteid')  ## 按照基站去重
        trace_data = pd.merge(trace_data, top1_site_data, left_on='enbid', right_on='siteid', how="left")
        tar_top1_site_data = top1_site_data.copy()
        tar_top1_site_data.rename(columns={"siteid": "tar_siteid", "top1_site_id": "tar_top1_site_id",
                                           "top1_site_distance": "tar_top1_site_distance",
                                           "sitedistance": "tar_sitedistance"}, inplace=True)
        trace_data = pd.merge(trace_data, tar_top1_site_data, left_on='destenbid', right_on='tar_siteid', how="left")
    else:
        trace_data["siteid"] = None
        trace_data["top1_site_id"] = None
        trace_data["top1_site_distance"] = None
        trace_data["sitedistance"] = None
        trace_data["tar_siteid"] = None
        trace_data["tar_top1_site_id"] = None
        trace_data["tar_top1_site_distance"] = None
        trace_data["tar_sitedistance"] = None
    trace_data["user_top1_site_id"] = None
    trace_data["user_top1_site_distance"] = None
    trace_data["description"] = None
    trace_data["classify"] = None
    trace_data["root_cause"] = None
    trace_data["action"] = None
    # trace_data = diagnosis_algrithom(trace_data)

    return trace_data


def multi_process(single_user_recorder, seed):
    # results = diagnosis_algrithom(single_user_recorder)

    max_row_num = single_user_recorder.shape[0]
    single_user_recorder.index = range(max_row_num)
    seed_num = seed
    pool = mp.Pool(seed_num)
    tasks = []
    step = math.ceil(max_row_num / seed_num)
    start_time = time.time()

    for i in range(seed_num):
        if (i + 1) * step >= max_row_num:
            row_start = i * step
            row_end = max_row_num
        else:
            row_start = i * step
            row_end = (i + 1) * step
        tasks.append(single_user_recorder.iloc[row_start:row_end, :])

    results = [pool.apply_async(diagnosis_algrithom, args=[task]) for task in tasks]

    results = [p.get() for p in results]

    for i in range(len(results)):
        if i == 0:
            dignosis_output = results[0]
        else:
            dignosis_output = pd.concat([dignosis_output, results[i]])
    end_time = time.time()
    print("单用户诊断用时{0}分钟".format((end_time - start_time) / 60))

    ## 数据类型转换
    dignosis_output['pci'] = dignosis_output['pci'].astype(pd.Int64Dtype())
    dignosis_output['totalsample'] = dignosis_output['totalsample'].astype(pd.Int64Dtype())
    dignosis_output['servnotbestsample'] = dignosis_output['servnotbestsample'].astype(pd.Int64Dtype())
    dignosis_output['poorcoveragesample'] = dignosis_output['poorcoveragesample'].astype(pd.Int64Dtype())
    dignosis_output['overlap3sample'] = dignosis_output['overlap3sample'].astype(pd.Int64Dtype())
    dignosis_output['overlap4sample'] = dignosis_output['overlap4sample'].astype(pd.Int64Dtype())

    dignosis_output['overshootingsample'] = dignosis_output['overshootingsample'].astype(pd.Int64Dtype())
    dignosis_output['servdeltadistr0'] = dignosis_output['servdeltadistr0'].astype(pd.Int64Dtype())
    dignosis_output['servdeltadistr1'] = dignosis_output['servdeltadistr1'].astype(pd.Int64Dtype())

    dignosis_output['servdeltadistr2'] = dignosis_output['servdeltadistr2'].astype(pd.Int64Dtype())
    dignosis_output['servdeltadistr3'] = dignosis_output['servdeltadistr3'].astype(pd.Int64Dtype())
    dignosis_output['servdeltadistr4'] = dignosis_output['servdeltadistr4'].astype(pd.Int64Dtype())

    dignosis_output['servdeltadistr5'] = dignosis_output['servdeltadistr5'].astype(pd.Int64Dtype())
    dignosis_output['servdeltadistr6'] = dignosis_output['servdeltadistr6'].astype(pd.Int64Dtype())
    dignosis_output['servdeltadistr7'] = dignosis_output['servdeltadistr7'].astype(pd.Int64Dtype())

    dignosis_output['nb1pci'] = dignosis_output['nb1pci'].astype(pd.Int64Dtype())
    dignosis_output['nb1sample'] = dignosis_output['nb1sample'].astype(pd.Int64Dtype())
    dignosis_output['nb2pci'] = dignosis_output['nb2pci'].astype(pd.Int64Dtype())

    dignosis_output['nb2sample'] = dignosis_output['nb2sample'].astype(pd.Int64Dtype())
    dignosis_output['nb3pci'] = dignosis_output['nb3pci'].astype(pd.Int64Dtype())
    dignosis_output['nb3sample'] = dignosis_output['nb3sample'].astype(pd.Int64Dtype())

    dignosis_output['interfreq'] = dignosis_output['interfreq'].astype(pd.Int64Dtype())
    dignosis_output['interfreqnbpci'] = dignosis_output['interfreqnbpci'].astype(pd.Int64Dtype())
    dignosis_output['taskid'] = dignosis_output['taskid'].astype(pd.Int64Dtype())

    dignosis_output['eci'] = dignosis_output['eci'].astype(pd.Int64Dtype())
    dignosis_output['tar_eci'] = dignosis_output['tar_eci'].astype(pd.Int64Dtype())

    return dignosis_output


def diagnosis_algrithom(single_user_recorder):
    endcause_list = {"0": "-", "50": "RRCRelease", "11": "RRCSetupFail", "12": "RRCSetupReject", "21": "SMCFail",
                     "40": "RRCReest",
                     "41": "RRCReestFail", "42": "RRCReestReject", "71": "IntraENBHoInFail", "80": "IntraENBHoOut",
                     "83": "IntraENBHoOutReest",
                     "101": "InitContexFail", "111": "S1HoInFail", "112": "S1HoInPrepFail", "113": "S1HoInCancel",
                     "120": "S1HoOut",
                     "123": "S1HoOutReest", "131": "X2HoInFail", "132": "X2HoInPrepFail", "133": "X2HoInCancel",
                     "140": "X2HoOut",
                     "143": "X2HoOutReest", "150": "S1Release", "160": "X2Release"}
    servicetype_list = {"1": "Data", "2": "Voice", "3": "RealtimeGaming", "4": "Vedio", "5": "IMSData"}
    connstatus_list = {"1": "NormalRelease", "2": "ConnFail", "3": "ConnDrop", "4": "NoEndMsg"}
    connesttype_list = {"0": "RRCSetup", "1": "RRCSetup", "2": "RRCReest", "3": "IntraENbHoIn", "100": "S1HoIn",
                        "200": "X2HoIn", "102": "S1HoInReest", "202": "X2HoInReest"}

    # indoor_list = {"0": "宏小区", "1": "室分小区"}
    for i in single_user_recorder.index:
        ## 添加预判字段
        description = ""
        classify = ""
        root_cause = ""
        action = ""

        destenbid = single_user_recorder.loc[i, "destenbid"]
        destcellid = single_user_recorder.loc[i, "destcellid"]

        servicetype = single_user_recorder.loc[i, "servicetype"]
        connstatus = single_user_recorder.loc[i, "connstatus"]
        endcause = single_user_recorder.loc[i, "endcause"]
        connesttype = single_user_recorder.loc[i, "connesttype"]

        src_indoor = single_user_recorder.loc[i, "indoor"]
        tar_indoor = single_user_recorder.loc[i, "tar_indoor"]

        # 异常事件到小区距离
        to_src_cell_distance = round(single_user_recorder.loc[i, "distance"], 2)
        to_tar_cell_distance = round(single_user_recorder.loc[i, "tar_distance"], 2)

        src_tilt = single_user_recorder.loc[i, "tilt"]
        tar_tilt = single_user_recorder.loc[i, "tar_tilt"]
        # 告警关联
        src_alarm_num = single_user_recorder.loc[i, "top1_alarm_num"]
        src_alarm_name = single_user_recorder.loc[i, "top1_alarm_name"]
        tar_alarm_num = single_user_recorder.loc[i, "tar_top1_alarm_num"]
        tar_alarm_name = single_user_recorder.loc[i, "tar_top1_alarm_name"]
        #  pci 混淆
        pci_confuse_flag = single_user_recorder.loc[i, "pci_confuse_flag"]
        pci_confuse_eci = single_user_recorder.loc[i, "pci_confuse_eci"]

        # 邻区漏配
        neighbour_mismatch_flag = single_user_recorder.loc[i, "neighbour_mismatch_flag"]
        neighbour_mismatch_eci = single_user_recorder.loc[i, "neighbour_mismatch_eci"]

        # 呼叫过程服务小区电平强度
        servrsrp = round(single_user_recorder.loc[i, "servrsrp"], 2)
        nb1rsrp = single_user_recorder.loc[i, "nb1rsrp"]
        nb2rsrp = single_user_recorder.loc[i, "nb2rsrp"]
        nb3rsrp = single_user_recorder.loc[i, "nb3rsrp"]
        interfreqnbrsrp = single_user_recorder.loc[i, "interfreqnbrsrp"]

        src_plansite_flag = single_user_recorder.loc[i, "plansite_flag"]
        tar_plansite_flag = single_user_recorder.loc[i, "tar_plansite_flag"]

        # pci = single_user_recorder.loc[i, "pci"]
        src_top1_site_id = single_user_recorder.loc[i, "top1_site_id"]
        src_top1_site_distance = single_user_recorder.loc[i, "top1_site_distance"]
        tar_top1_site_id = single_user_recorder.loc[i, "tar_top1_site_id"]
        tar_top1_site_distance = single_user_recorder.loc[i, "tar_top1_site_distance"]
        # 呼叫过程告警
        src_ul_sinr_avg = single_user_recorder.loc[i, "ul_sinr_avg"]

        src_idle_prb_noise_poser = single_user_recorder.loc[i, "idle_prb_noise_poser"]
        tar_idle_prb_noise_poser = single_user_recorder.loc[i, "tar_idle_prb_noise_poser"]
        src_conn_prb_noise_poser = single_user_recorder.loc[i, "rip_avg"]
        ##如果呼叫过程中小区上行干扰值大，则用该值去判断。
        if not pd.isnull(src_conn_prb_noise_poser) and src_conn_prb_noise_poser < 0:
            src_idle_prb_noise_poser = src_conn_prb_noise_poser

        # 呼叫过程重叠覆盖
        overlap = single_user_recorder.loc[i, "overlap_ratio"]

        # 呼叫过程越区采样点  越区占比
        overshootingsample = single_user_recorder.loc[i, "overshootingsample"]
        tar_overshooting_ratio = single_user_recorder.loc[i, "tar_overshooting_ratio"]

        user_top1_site_distance = single_user_recorder.loc[i, "user_top1_site_distance"]
        user_top1_site_id = single_user_recorder.loc[i, "user_top1_site_id"]

        src_avgsitedis = single_user_recorder.loc[i, "sitedistance"]
        tar_avgsitedis = single_user_recorder.loc[i, "tar_sitedistance"]

        if single_user_recorder.loc[i, "src_servingsample"] > 0:
            src_rsrpge110 = single_user_recorder.loc[i, "src_rsrp_ge110_sample"] / single_user_recorder.loc[
                i, "src_servingsample"]
        else:
            src_rsrpge110 = -1
        if single_user_recorder.loc[i, "tar_servingsample"] > 0:
            tar_rsrpge110 = single_user_recorder.loc[i, "tar_rsrp_ge110_sample"] / single_user_recorder.loc[
                i, "tar_servingsample"]
            tar_overlap_ratio = single_user_recorder.loc[i, "tar_overlap3sample"] / single_user_recorder.loc[
                i, "tar_servingsample"]
        else:
            tar_rsrpge110 = -1
        src_cellname = single_user_recorder.loc[i, "cellname"]
        tar_cellname = single_user_recorder.loc[i, "tar_cellname"]
        # 目标小区prb利用率
        tar_busy_dl_prb_use_ratio = single_user_recorder.loc[i, "tar_busy_dl_prb_use_ratio"]
        tar_busy_ul_prb_use_ratio = single_user_recorder.loc[i, "tar_busy_ul_prb_use_ratio"]
        src_busy_dl_prb_use_ratio = single_user_recorder.loc[i, "busy_dl_prb_use_ratio"]
        src_busy_ul_prb_use_ratio = single_user_recorder.loc[i, "busy_ul_prb_use_ratio"]
        src_ul_prb_total = single_user_recorder.loc[i, "ul_prb_total"]
        src_dl_prb_total = single_user_recorder.loc[i, "dl_prb_total"]
        src_dl_prb = single_user_recorder.loc[i, "dl_prb"]
        src_ul_prb = single_user_recorder.loc[i, "ul_prb"]
        ## 如果呼叫中携带的prb利用率不可用，就用小区忙时prb利用率
        if not pd.isnull(src_ul_prb_total) and src_ul_prb_total > 0:
            src_ul_prb_ratio = round(src_ul_prb / src_dl_prb_total, 2) * 100
            if src_ul_prb_ratio > 100:
                src_ul_prb_ratio = round(src_busy_ul_prb_use_ratio, 2) * 100
        else:
            src_ul_prb_ratio = round(src_busy_ul_prb_use_ratio, 2) * 100

        if not pd.isnull(src_dl_prb_total) and src_dl_prb_total > 0:
            src_dl_prb_ratio = round(src_dl_prb / src_dl_prb_total, 2) * 100
            if src_dl_prb_ratio > 100:
                src_dl_prb_ratio = round(src_busy_dl_prb_use_ratio, 2) * 100
        else:
            src_dl_prb_ratio = round(src_busy_dl_prb_use_ratio, 2) * 100

        ## 问题诊断及分敛

        if connstatus_list[str(connstatus)] == "ConnFail":
            ## 问题描述
            # if connesttype_list[str(connesttype)] == "RRCSetup":
            #     # 问题描述
            #     if servicetype_list[str(servicetype)] == "Voice" or servicetype_list[str(servicetype)] == "IMSData":
            #         description = "该呼叫记录业务类型为Voice业务，RRC建立类型为初始建立，但建立过程出现失败，携带原因值为" + endcause_list[str(endcause)]
            #     else:
            #         description = "该呼叫记录业务类型为" + servicetype_list[str(servicetype)] + "业务，RRC建立类型为初始建立，但建立过程出现失败，携带原因值为" \
            #                       + endcause_list[str(endcause)]
            # elif connesttype_list[str(connesttype)] == "RRCReest":
            #     if servicetype_list[str(servicetype)] == "Voice" or servicetype_list[str(servicetype)] == "IMSData":
            #         description = "该呼叫记录业务类型为Voice业务，RRC建立类型为RRC重建，但重建失败，携带原因值为" + endcause_list[str(endcause)]
            #     else:
            #         description = "该呼叫记录业务类型为" + servicetype_list[str(servicetype)] + "业务，RRC建立类型为RRC重建，但重建失败，携带原因值为" \
            #                       + endcause_list[str(endcause)]
            # else:
            #     if servicetype_list[str(servicetype)] == "Voice" or servicetype_list[str(servicetype)] == "IMSData":
            #         description = "该呼叫记录业务类型为Voice业务，用户从其他小区切至本小区，但切入失败，发生掉话，携带原因值为" + endcause_list[str(endcause)]
            #     else:
            #         description = "该呼叫记录业务类型为" + servicetype_list[str(servicetype)] + "业务，用户从其他小区切至本小区，但切入失败，发生掉线，携带原因值为" \
            #                       + endcause_list[str(endcause)]
            if connesttype_list[str(connesttype)] == "RRCSetup":
                # 问题描述
                if servicetype_list[str(servicetype)] == "Voice" or servicetype_list[str(servicetype)] == "IMSData":
                    description = "该呼叫记录的RRC建立类型为初始建立，但建立过程出现失败，携带原因值为" + endcause_list[str(endcause)]
                else:
                    description = "该呼叫记录的RRC建立类型为初始建立，但建立过程出现失败，携带原因值为" \
                                  + endcause_list[str(endcause)]
            elif connesttype_list[str(connesttype)] == "RRCReest":
                if servicetype_list[str(servicetype)] == "Voice" or servicetype_list[str(servicetype)] == "IMSData":
                    description = "该呼叫记录的RRC建立类型为RRC重建，但重建失败，携带原因值为" + endcause_list[str(endcause)]
                else:
                    description = "该呼叫记录的RRC建立类型为RRC重建，但重建失败，携带原因值为" \
                                  + endcause_list[str(endcause)]
            else:
                if servicetype_list[str(servicetype)] == "Voice" or servicetype_list[str(servicetype)] == "IMSData":
                    description = "用户从其他小区切至本小区但切入失败，发生掉话，携带原因值为" + endcause_list[str(endcause)]
                else:
                    description = "用户从其他小区切至本小区但切入失败，发生掉线，携带原因值为" \
                                  + endcause_list[str(endcause)]

            if not pd.isnull(src_alarm_num) and src_alarm_num > 0:
                classify = "维护"
                root_cause = "接入小区有重要告警,告警名称是" + src_alarm_name + "，告警次数" + str(src_alarm_num) + "次"
                action = "建议排查接入小区重要告警"

            elif not pd.isnull(servrsrp) and servrsrp < -110:
                if src_indoor == 1:
                    classify = "优化"
                    root_cause = "接入小区为室分小区，存在弱覆盖,接入电平" + str(servrsrp) + "dBm"
                    action = "建议排查接入小区弱覆盖区域，提升小区最大发射功率或者增加天线点位"
                else:
                    if not pd.isnull(src_plansite_flag) and src_plansite_flag == 1:
                        classify = "建设"
                        root_cause = "服务小区弱覆盖且周围有规划站"
                        action = "建议建设新站并尽快交维"
                    elif not pd.isnull(src_avgsitedis) and src_avgsitedis > 600 * 1.5:  ### 修改
                        classify = "优化"
                        root_cause = "问题区域平均站间距过大"
                        action = "建议规划新站"
                    elif not pd.isnull(to_src_cell_distance) and to_src_cell_distance < 100:
                        classify = "优化"
                        root_cause = "服务小区近距离弱覆盖"
                        action = "建议压低天线倾角，提升服务小区近距离覆盖能力"
                    elif to_src_cell_distance > 300 and to_src_cell_distance < 5000:
                        classify = "优化"
                        root_cause = "用户处于服务小区覆盖远点"
                        action = "建议抬天线或者增大发射功率，提升服务小区覆盖能力"
                    else:
                        classify = "优化"
                        root_cause = "用户使用业务的位置网络覆盖差,信息不足无法进一步定位"
                        action = "建议路测定位并整改问题，提升小区覆盖能力"
            elif not pd.isnull(overshootingsample) and overshootingsample > 0 and not pd.isnull(
                    to_src_cell_distance) and to_src_cell_distance > 1.5 * src_avgsitedis and to_src_cell_distance < 5000:  # src_avgsitedis 应该改为top1物理站站间距
                classify = "优化"
                root_cause = "服务小区越区覆盖，用户到服务小区距离" + str(to_src_cell_distance) + "米"
                action = "压低天线下倾角或者降低站高"
            elif not pd.isnull(src_idle_prb_noise_poser) and src_idle_prb_noise_poser > -95:
                classify = "维护"
                root_cause = "接入小区存在上行外部强干扰,上行闲时每PRB噪声功率为" + str(src_idle_prb_noise_poser) + "dBm"
                action = "建议排查外部强上行干扰"
            elif not pd.isnull(src_ul_sinr_avg) and src_ul_sinr_avg < -5:
                classify = "维护"
                root_cause = "接入小区存在上行干扰,上行sinr值为" + str(src_ul_sinr_avg) + "dBm"
                action = "建议排查上行干扰"
            elif (not pd.isnull(overlap) and overlap > 0.3):
                classify = "优化"
                root_cause = "接入小区重叠覆盖率高，下行干扰强"
                action = "建议优化天馈设置，控制覆盖范围，降低重叠覆盖率"
            elif (not pd.isnull(src_ul_prb_ratio) and src_ul_prb_ratio >= 50) or \
                    (not pd.isnull(src_dl_prb_ratio) and src_dl_prb_ratio >= 50):
                classify = "优化"
                root_cause = "服务小区资源受限，上行prb利用率" + str(src_ul_prb_ratio) + "%，下行prb利用率" + str(src_dl_prb_ratio) + "%"
                action = "建议服务小区扩容"
            else:
                classify = "-"
                root_cause = "信息不足无法确定根因"
                action = "建议现场测试或者回溯信令定位问题"

        elif connstatus_list[str(connstatus)] == "ConnDrop":  # or connstatus_list[str(connstatus)] == "NoEndMsg":
            # if connesttype_list[str(connesttype)] == "RRCSetup":
            #     # 问题描述
            #     if servicetype_list[str(servicetype)] == "Voice" or servicetype_list[str(servicetype)] == "IMSData":
            #         description = "该呼叫记录业务类型为Voice业务，RRC建立类型为初始建立，业务过程出现RRC异常释放，发生掉话，携带原因值为" + endcause_list[str(endcause)]
            #     else:
            #         description = "该呼叫记录业务类型为" + servicetype_list[str(servicetype)] + "业务，RRC建立类型为初始建立，业务过程中出现RRC异常释放，发生掉话，携带原因值为" \
            #                       + endcause_list[str(endcause)]
            # elif connesttype_list[str(connesttype)] == "RRCReest":
            #     if servicetype_list[str(servicetype)] == "Voice" or servicetype_list[str(servicetype)] == "IMSData":
            #         description = "该呼叫记录业务类型为Voice业务，RRC建立类型为RRC重建，业务过程中出现RRC异常释放，发生掉话，携带原因值为" + endcause_list[str(endcause)]
            #     else:
            #         description = "该呼叫记录业务类型为" + servicetype_list[str(servicetype)] + "业务，RRC建立类型为RRC重建，业务过程中出现RRC异常释放，发生掉线，携带原因值为" \
            #                       + endcause_list[str(endcause)]
            # else:
            #     if servicetype_list[str(servicetype)] == "Voice" or servicetype_list[str(servicetype)] == "IMSData":
            #         description = "该呼叫记录业务类型为Voice业务，用户从其他小区切至本小区，建立连接后业务过程中发生RRC异常释放，发生掉话，携带原因值为" + endcause_list[str(endcause)]
            #     else:
            #         description = "该呼叫记录业务类型为" + servicetype_list[str(servicetype)] + "业务，用户从其他小区切至本小区，建立连接后业务过程中发生RRC异常释放，发生掉线，携带原因值为" \
            #                       + endcause_list[str(endcause)]

            if connesttype_list[str(connesttype)] == "RRCSetup":
                # 问题描述
                if servicetype_list[str(servicetype)] == "Voice" or servicetype_list[str(servicetype)] == "IMSData":
                    description = "该呼叫记录的RRC建立类型为初始建立，业务过程出现RRC异常释放，发生掉话，携带原因值为" + endcause_list[
                        str(endcause)]
                else:
                    description = "该呼叫记录的RRC建立类型为初始建立，业务过程中出现RRC异常释放，发生掉线，携带原因值为" \
                                  + endcause_list[str(endcause)]
            elif connesttype_list[str(connesttype)] == "RRCReest":
                if servicetype_list[str(servicetype)] == "Voice" or servicetype_list[str(servicetype)] == "IMSData":
                    description = "该呼叫记录的RRC建立类型为RRC重建，业务过程中出现RRC异常释放，发生掉话，携带原因值为" + endcause_list[
                        str(endcause)]
                else:
                    description = "该呼叫记录的RRC建立类型为RRC重建，业务过程中出现RRC异常释放，发生掉线，携带原因值为" \
                                  + endcause_list[str(endcause)]
            else:
                if servicetype_list[str(servicetype)] == "Voice" or servicetype_list[str(servicetype)] == "IMSData":
                    description = "用户从其他小区成功切至本小区，业务过程中发生RRC异常释放，发生掉话，携带原因值为" + endcause_list[
                        str(endcause)]
                else:
                    description = "用户从其他小区成功切至本小区，业务过程中发生RRC异常释放，发生掉线，携带原因值为" \
                                  + endcause_list[str(endcause)]
            # 问题诊断
            if (not pd.isnull(src_alarm_num) and src_alarm_num > 0) or (
                    not pd.isnull(tar_alarm_num) and tar_alarm_num > 0):
                if src_alarm_num > 0 and tar_alarm_num > 0:
                    classify = "维护"
                    root_cause = "服务小区和目标小区均存在重要告警,服务小区告警名称是" + str(src_alarm_name) + "，目标小区告警名称是" + tar_alarm_name
                    action = "排查服务小区和目标小区重要告警"
                elif src_alarm_num > 0 and tar_alarm_num <= 0:
                    classify = "维护"
                    root_cause = "服务小区存在重要告警,服务小区告警名称是" + str(src_alarm_name)
                    action = "排查服务小区告警"
                elif src_alarm_num <= 0 and tar_alarm_num > 0:
                    classify = "维护"
                    root_cause = "切换目标小区存在重要告警,服务小区告警名称是" + str(tar_alarm_name)
                    action = "排查切换目标小区告警"
            elif not pd.isnull(neighbour_mismatch_flag) and neighbour_mismatch_flag == 1:
                classify = "优化"
                root_cause = "服务小区漏配" + str(neighbour_mismatch_eci) + "小区邻区关系"
                action = "建议添加服务小区与漏配邻区的邻区关系"
            elif not pd.isnull(pci_confuse_flag) and pci_confuse_flag == 1:
                classify = "优化"
                root_cause = "用户切换目标小区存在pci混淆，出现混淆的pci为" + str(pci_confuse_eci)
                action = "混淆pci重配置"
            elif (not pd.isnull(servrsrp) and servrsrp < -110) or (
                    not pd.isnull(src_rsrpge110) and src_rsrpge110 < 0.7):
                if (not pd.isnull(src_plansite_flag) and src_plansite_flag == 1) or \
                        (not pd.isnull(tar_plansite_flag) and tar_plansite_flag == 1):
                    classify = "建设"
                    root_cause = "服务小区弱覆盖且周围有规划站"
                    action = "建议加快站点建设"
                elif not pd.isnull(src_avgsitedis) and src_avgsitedis > 600 and src_indoor == 0:
                    classify = "优化"
                    root_cause = "问题区域平均站间距过大"
                    action = "建议规划新站"
                elif not pd.isnull(to_src_cell_distance) and to_src_cell_distance < 100:
                    classify = "优化"
                    root_cause = "服务小区近距离弱覆盖"
                    action = "建议压低天线倾角，提升服务小区近距离覆盖能力"
                elif not pd.isnull(to_src_cell_distance) and to_tar_cell_distance < 100 and not pd.isnull(
                        tar_rsrpge110) and tar_rsrpge110 < 0.7:
                    classify = "优化"
                    root_cause = "切换目标小区近距离弱覆盖"
                    action = "建议抬天线或者增大发射功率，提升服务小区覆盖能力"
                elif tar_rsrpge110 < 0.7 and to_tar_cell_distance > 300 and to_src_cell_distance > 300 and to_src_cell_distance < 5000:
                    classify = "优化"
                    root_cause = "用户处于服务小区和切换目标小区覆盖边缘，覆盖差"
                    action = "建议提升小区边缘覆盖能力，合理规划切换带"
                elif (not pd.isnull(src_indoor) and src_indoor == 1 and not pd.isnull(
                        src_cellname) and "电梯" in src_cellname) or \
                        (not pd.isnull(tar_indoor) and tar_indoor == 1 and not pd.isnull(
                            tar_cellname) and "电梯" in tar_cellname and tar_rsrpge110 < 0.7):
                    classify = "优化"
                    root_cause = "用户处于电梯覆盖场景，电梯与楼层切换区域覆盖差"
                    action = "建议提升小区边缘覆盖能力，合理规划切换带"
                else:
                    classify = "优化"
                    root_cause = "用户使用业务的位置网络覆盖差,信息不足无法进一步定位"
                    action = "建议路测定位并整改问题，提升小区覆盖能力"
            elif not pd.isnull(overshootingsample) and overshootingsample > 0 and not pd.isnull(
                    to_src_cell_distance) and to_src_cell_distance > 1.5 * src_avgsitedis and to_src_cell_distance < 5000:
                classify = "优化"
                root_cause = "服务小区越区覆盖，用户到服务小区距离" + str(to_src_cell_distance) + "米"
                action = "压低天线下倾角或者降低站高"
            elif not pd.isnull(src_idle_prb_noise_poser) and src_idle_prb_noise_poser > -95:
                classify = "优化"
                root_cause = "服务小区存在上行外部强干扰,上行闲时每PRB噪声功率为" + str(src_idle_prb_noise_poser) + "dBm"
                action = "建议排查外部强上行干扰"
            elif not pd.isnull(src_ul_sinr_avg) and src_ul_sinr_avg < -5:
                classify = "优化"
                root_cause = "服务小区存在上行,上行sinr值为" + str(src_ul_sinr_avg) + "dBm"
                action = "建议排查目标小区上行干扰"
            elif not pd.isnull(tar_idle_prb_noise_poser) and tar_idle_prb_noise_poser > -95:
                classify = "维护"
                root_cause = "切换目标小区存在上行强干扰，上行闲时每PRB噪声功率为" + tar_idle_prb_noise_poser + "dBm"
                action = "建议排查目标小区上行外部干扰"
            elif (not pd.isnull(overlap) and overlap > 0.3) or (not pd.isnull(overlap) and tar_overlap_ratio > 0.3):
                classify = "优化"
                root_cause = "问题区域重叠覆盖率高，下行干扰强"
                action = "建议优化天馈，控制覆盖范围，降低重叠覆盖率"
            elif (not pd.isnull(tar_busy_ul_prb_use_ratio) and tar_busy_ul_prb_use_ratio > 50) or (
                    not pd.isnull(tar_busy_dl_prb_use_ratio) and tar_busy_dl_prb_use_ratio > 50):
                classify = "优化"
                root_cause = "切换目标小区PRB资源受限,上行PRB利用率为" + str(tar_busy_ul_prb_use_ratio) + "%,下行PRB利用率为" + str(
                    tar_busy_dl_prb_use_ratio) + "%"
                action = "建议目标小区扩容"
            else:
                classify = "-"
                root_cause = "信息不足无法确定根因"
                action = "建议现场测试或者回溯信令定位问题"
        else:
            classify = "-"
            root_cause = "信息不足无法确定根因"
            action = "建议现场测试或者回溯信令定位问题"

        ## 赋值
        single_user_recorder.loc[i, "description"] = description
        single_user_recorder.loc[i, "classify"] = classify
        single_user_recorder.loc[i, "root_cause"] = root_cause
        single_user_recorder.loc[i, "action"] = action
    return single_user_recorder


def date_modify(tablename, delta_ms):
    conn = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502",
                            host="10.1.77.51", port="5432")
    print("exppert database Connection success")

    cur = conn.cursor()

    sql = "update " + table + " set endtime =endtime+" + str(
        delta_ms) + ";" + "update " + table + " set starttime =starttime+" + str(delta_ms) + ";"

    cur.execute(sql, [tablename, delta_ms, tablename, delta_ms])
    conn.commit()


if __name__ == '__main__':
    ## 北京数据库账密
    trace_db_account = {"database": "trace-tables", "user": "trace-tables", "password": "YJY_tra#tra502",
                        "host": "10.1.77.51", "port": "5432"}
    expert_db_account = {"database": "expert-system", "user": "expert-system", "password": "YJY_exp#exp502",
                         "host": "10.1.77.51", "port": "5432", "options": "-c search_path=cnio"}

    ## 郑州数据库账密
    # trace_db_account = {"database":"sk_data", "user":"sk","password":"CTRO4I!@#=",
    #                  "host":"133.160.191.108","port":"5432"}
    # expert_db_account = {"database":"expert-system", "user":"expert-system","password":"YJY_exp#exp502",
    #                  "host":"133.160.191.111","port":"5432","options":"-c search_path=cnio"}

    yestoday, yestoday_another, year, delta_ms = get_date()
    # 获取pm 告警数据
    pm_data, alarm_data, plansite_data, top1_site_data, siteinfo_data = get_analysis_data(yestoday, year,expert_db_account)

    ## 建表
    expert_create_talbe(yestoday,expert_db_account)
    ## 数据查询
    trace_data = get_call_recorder(yestoday, yestoday_another,trace_db_account)

    data_association = single_user_call_daignosis(trace_data, pm_data, alarm_data, plansite_data, top1_site_data,
                                                  siteinfo_data)

    ## 进程数
    seednum = 20
    table = 'lte_abnormal_call_detail_' + str(yestoday)

    if len(data_association) != 0:
        dignosis_output = multi_process(data_association, seednum)
        ## 数据
        data_load(dignosis_output, table,expert_db_account)

        table = "cnio." + table
        date_modify(table, delta_ms)
        print("call detail date modify:success")
