import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import datetime as dt
from io import StringIO

"""
开发人员：qiaojj
代码功能：完成基于trace的用户呼叫诊断，并同步数据
修改时间：2022-10-24
"""
def get_date():
    today = dt.datetime.today()
    yestoday = today + dt.timedelta(days=(-2))

    yestoday_time = yestoday.strftime('%Y%m%d')
    yestoday_time_v = yestoday.strftime('%Y-%m-%d')

    return yestoday_time, yestoday_time_v


def get_pm_alarm(yestoday):
    conn = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502",
                            host="10.1.77.51", port="5432")
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

    # 获取告警数据
    sql_pm = """
    select eci,(important_alarm_num+urgent_alarm_num) as alarm_totalnum 
    from searching.alarm_hz_day where data_time = '2022-10-20'
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

    return pm_hour_data_pd, alarm_day_data_pd


def get_call_recorder(yestoday, yestoday_v):
    # conn = psycopg2.connect(database="sk_data", user="sk", password="CTRO4I!@#=",
    #                         host="133.160.191.108", port="5432")
    conn = psycopg2.connect(database="trace-tables", user="trace-tables", password="YJY_tra#tra502",
                            host="10.1.77.51", port="5432")
    print("Trace Postgres Connection success")

    cur = conn.cursor()
    # 获取taskid
    sql_trace_sk_task = """
    select taskid from public.sk_task where file_filter =%s and status='完成'
    """
    cur.execute(sql_trace_sk_task, [yestoday])
    taskid = cur.fetchall()

    data = []
    ## 如果taskid不为空，
    if len(taskid) == 0:
        pass
    else:
        taskid = pd.DataFrame(taskid)
        taskid.columns = ["taskid"]

        id = int(taskid.loc[0, "taskid"])

        sql = """
                select
            *
        from
            (
            select
                g.*,
                %s taskid,
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
                                (connesttype = 2
                                    or connesttype = 102
                                    or connesttype = 202
                                    or endcause = 71
                                    or endcause = 111
                                    or endcause = 112
                                    or endcause = 113
                                    or endcause = 123
                                    or endcause = 131
                                    or endcause = 132
                                    or endcause = 133
                                    or endcause = 143 )
                                and lon>0
                            limit 10000
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
        limit 10

        """
        cur.execute(sql, [id, id, id, id, id, id])

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


def expert_create_talbe(yestoday):
    conn = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502",
                            host="10.1.77.51", port="5432")

    print("Postgres Connection success")
    cur = conn.cursor()

    sql_drop_table = "drop table if exists cnio.dwd_abnormal_call_detail_" + yestoday
    sql_create_table = """
        create table cnio.dwd_abnormal_call_detail_%s(
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
        earfcn	int4	,
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
        ul_sinrdistr0		float4	,
        ul_sinrdistr1		float4	,
        ul_sinrdistr2		float4	,
        ul_sinrdistr3		float4	,
        ul_sinrdistr4		float4	,
        ul_sinrdistr5		float4	,
        ul_sinrdistr6		float4	,
        ul_sinrdistr7		float4	,
        rip_avg	float4	,
        rip_num		float4,
        ul_user_plane_bytes		float4	,
        dl_user_plane_bytes		float4	,
        ul_prb_drb		float4	,
        dl_prb_drb		float4	,
        dl_prb		float4	,
        ul_prb		float4	,
        ul_prb_total		float4	,
        dl_prb_total		float4	,
        dl_pdcp_sdu_packs_lost_drb		float4	,
        dl_pdcp_sdu_packs_discard_drb		float4	,
        dl_pdcp_sdu_packs_tx_drb		float4	,
        dl_pdcp_bytes		float4	,
        ul_pdcp_bytes		float4	,
        ul_pdcp_sdu_packs_lost_drb		float4	,
        ul_pdcp_sdu_packs_expected_drb		float4	,
        ul_tbs_qpsk		float4	,
        ul_tbs_16qam		float4	,
        ul_tbs_64qam		float4	,
        dl_tbs_qpsk		float4	,
        dl_tbs_16qam		float4	,
        dl_tbs_64qam		float4	,
        cqinum		float4,
        cqidistr0		float4	,
        cqidistr1		float4	,
        cqidistr2		float4	,
        rinum		float4	,
        ri2num		float4	,
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
        taskid	float8	,
        tar_servingsample	float8	,
        tar_servingrsrp	float8	,
        tar_rsrp_ge110_sample	float8	,
        tar_rsrp_ge115_sample	float8	,
        tar_rsrp_ge105_sample	float8	,
        tar_overlap3sample	float8	,
        tar_overlap4sample	float8	,
        tar_overshootingsample	float8	,
        tar_poorcoveragesample	float8	,
        tar_mod3sample	float8,
        busy_ul_prb_use_ratio  float8,
        busy_dl_prb_use_ratio float8,
        idle_prb_noise_poser float8,
        tar_busy_ul_prb_use_ratio float8,
        tar_busy_dl_prb_use_ratio float8,
        tar_idle_prb_noise_poser float8,
        alarm_totalnum float8,
        tar_alarm_totalnum float8,
        overlap_ratio float8,
        tar_overshooting_ratio float8,
        classify text,
        root_cause text,
        action text
        );
    """ % yestoday
    cur.execute(sql_drop_table)
    conn.commit()
    cur.execute(sql_create_table)
    conn.commit()
    cur.close()
    conn.close()
    print("create tablee: success")


def data_load(data, table):
    output = StringIO()
    data.to_csv(output, sep='\t', index=False, header=False)
    output1 = output.getvalue()
    conn = psycopg2.connect(host="10.1.77.51", user="expert-system", password="YJY_exp#exp502",
                            database="expert-system", port="5432", options="-c search_path=cnio")
    # options="-c search_path=cnio"
    cur = conn.cursor()
    cur.copy_from(StringIO(output1), table, sep='\t', null='')
    conn.commit()
    cur.close()
    conn.close()
    print("abnormal_call sync: success")


def single_user_call_daignosis(trace_data, pm_data, alarm_data):
    trace_data["eci"] = trace_data[["enbid", "cellid"]].apply(lambda x: x["enbid"] * 256 + x["cellid"], axis=1)
    trace_data["tar_eci"] = trace_data[["destenbid", "destcellid"]].apply(
        lambda x: x["destenbid"] * 256 + x["destcellid"], axis=1)

    single_user_recorder = pd.merge(trace_data, pm_data, on=["eci"], how="left")
    tar_pm_data = pm_data.copy()
    tar_pm_data.rename(columns={"eci": "tar_eci", "idle_prb_noise_poser": "tar_idle_prb_noise_poser",
                                "busy_ul_prb_use_ratio": "tar_busy_ul_prb_use_ratio",
                                "busy_dl_prb_use_ratio": "tar_busy_dl_prb_use_ratio"
                                }, inplace=True)

    single_user_recorder = pd.merge(single_user_recorder, tar_pm_data, on=["tar_eci"], how="left")

    single_user_recorder = pd.merge(single_user_recorder, alarm_data, on=["eci"], how="left")
    tar_alarm_data = alarm_data.copy()
    tar_alarm_data.rename(columns={"eci": "tar_eci", "alarm_totalnum": "tar_alarm_totalnum"}, inplace=True)
    single_user_recorder = pd.merge(single_user_recorder, tar_alarm_data, on=["tar_eci"], how="left")

    single_user_recorder["overlap_ratio"] = single_user_recorder[["overlap3sample", "totalsample"]].apply(
        lambda x: x["overlap3sample"] / x["totalsample"] if x["totalsample"] > 0 else -1, axis=1)

    single_user_recorder["tar_overshooting_ratio"] = single_user_recorder[
        ["tar_overshootingsample", "tar_servingsample"]].apply(
        lambda x: x["tar_overshootingsample"] / x["tar_servingsample"]
        if x["tar_servingsample"] > 0 else -1, axis=1)

    single_user_recorder["classify"] = ""
    single_user_recorder["root_cause"] = ""
    single_user_recorder["action"] = ""
    for i in single_user_recorder.index:
        endcause = single_user_recorder.loc[i, "endcause"]
        alarm = single_user_recorder.loc[i, "alarm_totalnum"]
        tar_alarm = single_user_recorder.loc[i, "tar_alarm_totalnum"]
        servrsrp = single_user_recorder.loc[i, "servrsrp"]
        nb1rsrp = single_user_recorder.loc[i, "nb1rsrp"]
        nb2rsrp = single_user_recorder.loc[i, "nb2rsrp"]
        nb3rsrp = single_user_recorder.loc[i, "nb3rsrp"]
        interfreqnbrsrp = single_user_recorder.loc[i, "interfreqnbrsrp"]
        pci = single_user_recorder.loc[i, "pci"]
        ul_sinr_avg = single_user_recorder.loc[i, "ul_sinr_avg"]
        idle_prb_noise_poser = single_user_recorder.loc[i, "idle_prb_noise_poser"]
        tar_idle_prb_noise_poser = single_user_recorder.loc[i, "tar_idle_prb_noise_poser"]
        overlap = single_user_recorder.loc[i, "overlap_ratio"]
        overshootingsample = single_user_recorder.loc[i, "overshootingsample"]
        tar_overshooting_ratio = single_user_recorder.loc[i, "tar_overshooting_ratio"]
        tar_busy_dl_prb_use_ratio = single_user_recorder.loc[i, "busy_dl_prb_use_ratio"]
        if endcause == 123 or endcause == 124 or endcause == 143 or endcause == 144:
            single_user_recorder.loc[i, "classify"], single_user_recorder.loc[i, "root_cause"], \
            single_user_recorder.loc[i, "action"] = ho_problem_dignosis(alarm, tar_alarm, servrsrp, nb1rsrp,
                                                                        nb2rsrp, nb3rsrp, interfreqnbrsrp, pci,
                                                                        ul_sinr_avg,
                                                                        idle_prb_noise_poser,
                                                                        tar_idle_prb_noise_poser, overlap,
                                                                        overshootingsample,
                                                                        tar_overshooting_ratio,
                                                                        tar_busy_dl_prb_use_ratio)
        else:
            single_user_recorder.loc[i, "classify"], single_user_recorder.loc[i, "root_cause"], \
            single_user_recorder.loc[i, "action"] = not_ho_problem_dignosis(alarm, servrsrp, nb1rsrp, nb2rsrp, nb3rsrp,
                                                                            interfreqnbrsrp, ul_sinr_avg,
                                                                            idle_prb_noise_poser, overlap,
                                                                            overshootingsample)
    return single_user_recorder


def ho_problem_dignosis(alarm, tar_alarm, servrsrp, nb1rsrp, nb2rsrp, nb3rsrp, interfreqnbrsrp, pci, ul_sinr_avg,
                        idle_prb_noise_poser, tar_idle_prb_noise_poser, overlap, overshootingsample,
                        tar_overshooting_ratio, tar_busy_dl_prb_use_ratio):
    if alarm > 0 or tar_alarm > 0:
        classify = "维护"
        root_cause = "服务小区或目标小区有紧急告警"
        action = "排查告警"
    elif overshootingsample > 0 or tar_overshooting_ratio > 0.05:
        classify = "优化"
        root_cause = "服务小区或目标存在越区覆盖问题"
        action = "调整天馈优化小区覆盖"
    elif servrsrp < -115 and nb1rsrp < -115 and nb2rsrp < -115 and nb3rsrp < -115 and interfreqnbrsrp:
        classify = "优化/建设"
        root_cause = "服务小区弱覆盖且无可切换邻区"
        action = "优化小区覆盖或补站"
    elif ul_sinr_avg < -5:
        if idle_prb_noise_poser < -100 or tar_idle_prb_noise_poser < -100:
            classify = "优化"
            root_cause = "存在外部强干扰"
            action = "排查外部干扰"
        else:
            classify = "优化"
            root_cause = "存在强干扰"
            action = "优先排查内部干扰（排查天馈器件）"
    elif overlap > 0.3:
        classify = "优化"
        root_cause = "重叠覆盖率高"
        action = "优化天馈控制覆盖范围"
    elif tar_overshooting_ratio > 0.5 or tar_busy_dl_prb_use_ratio > 0.5:
        classify = "优化/建设"
        root_cause = "目标小区高负荷"
        action = "优化覆盖范围或者建设新站"
    else:
        classify = "-"
        root_cause = "信息不足无法定位根因"
        action = "回溯信令或路测排查"
    return classify, root_cause, action


def not_ho_problem_dignosis(alarm, servrsrp, nb1rsrp, nb2rsrp, nb3rsrp, interfreqnbrsrp, ul_sinr_avg,
                            idle_prb_noise_poser, overlap, overshootingsample):
    if alarm > 0:
        classify = "维护"
        root_cause = "服务小区有紧急告警"
        action = "排查告警"
    elif overshootingsample > 0:
        classify = "优化"
        root_cause = "越区覆盖"
        action = "调整天馈优化小区覆盖"
    elif servrsrp < -115 and nb1rsrp < -115 and nb2rsrp < -115 and nb3rsrp < -115 and interfreqnbrsrp:
        classify = "优化/建设"
        root_cause = "弱覆盖区域"
        action = "优化小区覆盖或补站"
    elif ul_sinr_avg < -5:
        if idle_prb_noise_poser < -100:
            classify = "优化"
            root_cause = "存在外部强干扰"
            action = "排查外部干扰"
        else:
            classify = "优化"
            root_cause = "存在强干扰"
            action = "优先排查内部干扰（排查天馈器件）"
    elif overlap > 0.3:
        classify = "优化"
        root_cause = "重叠覆盖率高"
        action = "优化天馈控制覆盖范围"
    else:
        classify = "-"
        root_cause = "信息不足无法定位根因"
        action = "回溯信令或路测排查"
    return classify, root_cause, action


if __name__ == '__main__':
    yestoday, yestoday_v = get_date()
    # 获取pm 告警数据
    pm_data, alarm_data = get_pm_alarm(yestoday)

    ## 建表
    expert_create_talbe(yestoday)
    ## 数据查询
    trace_data = get_call_recorder(yestoday, yestoday_v)

    dignosis = single_user_call_daignosis(trace_data, pm_data, alarm_data)

    dignosis = dignosis.loc[:, ~dignosis.columns.isin(["eci", "tar_eci"])]

    ## 数据上传
    table = 'dwd_abnormal_call_detail_' + str(yestoday)
    if len(dignosis) != 0:
        data_load(dignosis, table)
