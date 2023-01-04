import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import datetime as dt
from io import StringIO

def get_date():
    today  = dt.datetime.today()
    yestoday = today + dt.timedelta(days=(-1 ))

    #today_time = today.strftime('%Y%m%d')
    yestoday_time = yestoday.strftime('%Y-%m-%d')
    #yestoday_time_extra=yestoday.strftime('%Y%m%d')
    return yestoday_time

def postgre_execute(yestoday):
    conn = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502",
                            host="10.1.77.51", port="5432")
    print("Postgres Connection open")

    cur = conn.cursor()

    sql ="""

select
	c.*,
	d.total_num,
	d.important_alarm_num,
	d.urgent_alarm_num,
	d.less_important_alarm_num,
	d.hint_alarm_num,
	d.top1_alarm_name,
	d.top1_alarm_num,
	d.top2_alarm_name,
	d.top2_alarm_num,
	d.top3_alarm_name,
	d.top3_alarm_num,
	d.data_time
from
	(
	select
		a.*,
		b.idletime_prb_noise_power_dbm ,
		b.cell_avail_ratio ,
		b.ul_busy_prb_use_ratio ,
		b.dl_busy_prb_use_ratio ,
		b.ul_traffic_gb ,
		b.dl_traffic_gb ,
		b.trace_date ,
		b.ul_pdcp_throughput ,
		b.dl_pdcp_throughput ,
		b.ul_prb_use_num ,
		b.ul_prb_avail_num ,
		b.dl_prb_use_num ,
		b.dl_prb_avail_num ,
		b.cell_not_avail_duration_ms ,
		b.avg_rrc_num ,
		b.volte_num
	from
		(
		select
			cellname,
			cellindex,
			pci,
			earfcn,
			eci,
			enbid,
			cellid,
			celllon,
			celllat,
			hbwd,
			azimuth,
			height,
			indoor,
			etilt,
			mtilt,
			vendor,
			province,
			city,
			district,
			scene,
			covertype,
			siteavgdist,
			celltype,
			indoorleak,
			overshooting,
			insuffcover,
			azimuthcheck,
			locationerr,
			maxdist,
			mindist,
			avgdist,
			totalmrsample_all,
			rsrp_all,
			bestsample_all,
			bestrsrp_all,
			servingsample_all,
			servingrsrp_all,
			overlap3sample_all,
			overlap4sample_all,
			overshootingsample_all,
			poorcoveragesample_all,
			servdeltam3sample_all,
			servnotbestsample_all,
			servdelta3sample_all,
			interferencesample_all,
			interfdelta3sample_all,
			mod3sample_all,
			totalmrsample,
			rsrp,
			bestsample,
			bestrsrp,
			servingsample,
			servingrsrp,
			overshootingsample,
			poorcoveragesample,
			overlap3sample,
			overlap4sample,
			servdeltam3sample,
			servnotbestsample,
			servdelta3sample,
			interferencesample,
			interfdelta3sample,
			mod3sample,
			totalgridnum,
			servingnum,
			overshootingnum,
			poorcoveragenum,
			overlap3num,
			overlap4num,
			servdelta3mnum,
			servnotbestnum,
			servdelta3num,
			interferencenum,
			interfdelta3num,
			conntotalnum,
			connreqnum,
			connfailnum,
			conndropnum,
			noendmsgnum,
			reestreqnum,
			reestendnum,
			redirgunum,
			redirnrnum,
			hooutgunum,
			hooutnrnum,
			uecapnrnum,
			nbcellnum,
			hoouttotalnum,
			hooutsuccnum,
			hooutprepfailnum,
			hooutexefailnum,
			hooutsrcreestnum,
			hooutdestreestnum,
			servcellnum,
			hointotalnum,
			hoinsuccnum,
			hoinprepfailnum,
			hoinexefailnum,
			hoinsrcreestnum,
			hoindestreestnum,
			pciconfservnum,
			pciconfhoouttotalnum,
			pciconfhooutsuccnum,
			pciconfhooutprepfailnum,
			pciconfhooutexefailnum,
			pciconfhooutsrcreestnum,
			pciconfhooutdestreestnum,
			pciconfdestnum,
			pciconfhointotalnum,
			pciconfhoinsuccnum,
			pciconfhoinprepfailnum,
			pciconfhointexefailnum,
			pciconfhoinsrcreestnum,
			pciconfhoindestreestnum,
			nbcfgcellnum,
			interfercellnum,
			mode3cellnum,
			ul16qamdistr,
			dl64qamdistr,
			ul16qam,
			ultotalmodtbs,
			dl64qam,
			dltotalmodtbs,
			uldrbprbusage,
			dldrbprbusage,
			uldrbprb,
			dldrbprb,
			ultotalprb,
			dltotalprb,
			ulretxrate,
			dlretxrate,
			ulretxtb,
			dlretxtb,
			ultotaltbs,
			dltotaltbs,
			servdelta3bytesdistr,
			servdelta3ulbytesdistr,
			servdelta3dlbytesdistr,
			intfdelta3bytesdistr,
			intfdelta3ulbytesdistr,
			intfdelta3dlbytesdistr,
			bytesgrade,
			avgbytes,
			ulbytes,
			dlbytes,
			totalbytes,
			cqil7distr,
			cqi0_6,
			totalcqinum
		from
			searching.trace_lte_cell_sum_day
		where
			data_date = '2022-10-18' ) a
	left join (
		select
			*
		from
			cnio.pm_4g_workorder
		where
			trace_date = '2022-10-18') b on
		a.eci = b.eci) c
left join (
	select
		*
	from
		searching.alarm_hz_day ahd
	where
		data_time = '2022-10-18' ) d on
	c.eci = d.eci 
    """


    cur.execute(sql)

    data = cur.fetchall()
    if len(data) !=0:
        data = pd.DataFrame(data)
        des = cur.description
        colname=[]
        for item in des:
            colname.append(item[0])
        data.columns=colname
    cur.close()
    conn.close()
    print("Postgres Connection close")
    return data

def postgre_upload(data):
    engine = create_engine('postgresql://expert-system:YJY_exp#exp502@10.1.77.51:5432/expert-system')
    data.to_sql("trace_lte_cell_sum_day",engine,schema="searching", index=False, if_exists='append',method='multi')
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
    yestoday = get_date()
    data = postgre_execute(yestoday)
    #data.to_csv("trace_mr_cell.csv")
    # if len(data)!=0:
    #     postgre_upload(data)

