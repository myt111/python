import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import datetime as dt
from io import StringIO

def get_date():
    today  = dt.datetime.today()
    yestoday = today + dt.timedelta(days=(-19 ))

    #today_time = today.strftime('%Y%m%d')
    yestoday_time = yestoday.strftime('%Y-%m-%d')
    yestoday_time_extra=yestoday.strftime('%Y%m%d')
    return yestoday_time_extra

def postgre_execute(yestoday_time_extra):
    # conn = psycopg2.connect(database="sk_data", user="sk", password="CTRO4I!@#=",
    #                         host="133.160.191.108", port="5432")
    # conn = psycopg2.connect(database="sk_data", user="sk", password="YJY_tra#tra502",
    #                         host="10.1.77.51", port="5432")

    conn = psycopg2.connect(database="trace-tables", user="trace-tables", password="YJY_tra#tra502",
                                                  host="10.1.77.51", port="5432")

    print("Trace Postgres Connection success")

    cur = conn.cursor()
    # 获取taskid
    sql_trace_sk_task  = """
    select taskid from public.sk_task where file_filter =%s and status='完成'
    """
    cur.execute(sql_trace_sk_task, [yestoday_time_extra])
    taskid = cur.fetchall()
    data =""
    ## 如果taskid不为空，
    if len(taskid)==0:
        pass
    else:
        taskid  = pd.DataFrame(taskid)
        taskid.columns = ["taskid"]

        sql = "select cellname	,	"+\
        "	cellindex	,	"+\
        "	pci	,	"+\
        "	earfcn	,	"+\
        "	eci	,	"+\
        "	enbid	,	"+\
        "	cellid	,	"+\
        "	celllon	,	"+\
        "	celllat	,	"+\
        "	hbwd	,	"+\
        "	azimuth	,	"+\
        "	height	,	"+\
        "	indoor	,	"+\
        "	etilt	,	"+\
        "	mtilt	,	"+\
        "	vendor	,	"+\
        "	province	,	"+\
        "	city	,	"+\
        "	district	,	"+\
        "	scene	,	"+\
        "	covertype	,	"+\
        "	siteavgdist	,	"+\
        "	celltype	,	"+\
        "	indoorleak	,	"+\
        "	overshooting	,	"+\
        "	insuffcover	,	"+\
        "	azimuthcheck	,	"+\
        "	locationerr	,	"+\
        "	maxdist	,	"+\
        "	mindist	,	"+\
        "	avgdist	,	"+\
        "	totalmrsample_all	,	"+\
        "	rsrp_all	,	"+\
        "	bestsample_all	,	"+\
        "	bestrsrp_all	,	"+\
        "	servingsample_all	,	"+\
        "	servingrsrp_all	,	"+\
        "	overlap3sample_all	,	"+\
        "	overlap4sample_all	,	"+\
        "	overshootingsample_all	,	"+\
        "	poorcoveragesample_all	,	"+\
        "	servdeltam3sample_all	,	"+\
        "	servnotbestsample_all	,	"+\
        "	servdelta3sample_all	,	"+\
        "	interferencesample_all	,	"+\
        "	interfdelta3sample_all	,	"+\
        "	mod3sample_all	,	"+\
        "	totalmrsample	,	"+\
        "	rsrp	,	"+\
        "	bestsample	,	"+\
        "	bestrsrp	,	"+\
        "	servingsample	,	"+\
        "	servingrsrp	,	"+\
        "	overshootingsample	,	"+\
        "	poorcoveragesample	,	"+\
        "	overlap3sample	,	"+\
        "	overlap4sample	,	"+\
        "	servdeltam3sample	,	"+\
        "	servnotbestsample	,	"+\
        "	servdelta3sample	,	"+\
        "	interferencesample	,	"+\
        "	interfdelta3sample	,	"+\
        "	mod3sample	,	"+\
        "	totalgridnum	,	"+\
        "	servingnum	,	"+\
        "	overshootingnum	,	"+\
        "	poorcoveragenum	,	"+\
        "	overlap3num	,	"+\
        "	overlap4num	,	"+\
        "	servdelta3mnum	,	"+\
        "	servnotbestnum	,	"+\
        "	servdelta3num	,	"+\
        "	interferencenum	,	"+\
        "	interfdelta3num	,	"+\
        "	conntotalnum	,	"+\
        "	connreqnum	,	"+\
        "	connfailnum	,	"+\
        "	conndropnum	,	"+\
        "	noendmsgnum	,	"+\
        "	reestreqnum	,	"+\
        "	reestendnum	,	"+\
        "	redirgunum	,	"+\
        "	redirnrnum	,	"+\
        "	hooutgunum	,	"+\
        "	hooutnrnum	,	"+\
        "	uecapnrnum	,	"+\
        "	nbcellnum	,	"+\
        "	hoouttotalnum	,	"+\
        "	hooutsuccnum	,	"+\
        "	hooutprepfailnum	,	"+\
        "	hooutexefailnum	,	"+\
        "	hooutsrcreestnum	,	"+\
        "	hooutdestreestnum	,	"+\
        "	servcellnum	,	"+\
        "	hointotalnum	,	"+\
        "	hoinsuccnum	,	"+\
        "	hoinprepfailnum	,	"+\
        "	hoinexefailnum	,	"+\
        "	hoinsrcreestnum	,	"+\
        "	hoindestreestnum	,	"+\
        "	pciconfservnum	,	"+\
        "	pciconfhoouttotalnum	,	"+\
        "	pciconfhooutsuccnum	,	"+\
        "	pciconfhooutprepfailnum	,	"+\
        "	pciconfhooutexefailnum	,	"+\
        "	pciconfhooutsrcreestnum	,	"+\
        "	pciconfhooutdestreestnum	,	"+\
        "	pciconfdestnum	,	"+\
        "	pciconfhointotalnum	,	"+\
        "	pciconfhoinsuccnum	,	"+\
        "	pciconfhoinprepfailnum	,	"+\
        "	pciconfhointexefailnum	,	"+\
        "	pciconfhoinsrcreestnum	,	"+\
        "	pciconfhoindestreestnum	,	"+\
        "	nbcfgcellnum	,	"+\
        "	interfercellnum	,	"+\
        "	mode3cellnum	,	"+\
        "	(case when ultotalmodtbs>0 then (ul16qam/ultotalmodtbs)*100 else NULL end) as  ul16qamdistr	,	"+\
        "	(case when dltotalmodtbs>0 then (dl64qam/dltotalmodtbs)*100	else NULL end) as dl64qamdistr	,"+\
        "	ul16qam	,	"+\
        "	ultotalmodtbs	,	"+\
        "	dl64qam	,	"+\
        "	dltotalmodtbs	,	"+\
        "	(case when ultotalprb>0 then (uldrbprb/ultotalprb)*100 else NULL end)  as uldrbprbusage	,	"+\
        "	(case when dltotalprb>0 then (dldrbprb/dltotalprb)*100 else NULL end)  as dldrbprbusage	,	"+\
        "	uldrbprb	,	"+\
        "	dldrbprb	,	"+\
        "	ultotalprb	,	"+\
        "	dltotalprb	,	"+\
        "	(case when ultotaltbs>0 then (ulretxtb/ultotaltbs)*100  else NULL end) as ulretxrate	,	"+\
        "	(case when dltotaltbs>0 then (dlretxrate/dltotaltbs)*100  else NULL end)	 as  dlretxrate	, "+\
        "	ulretxtb	,	"+\
        "	dlretxtb	,	"+\
        "	ultotaltbs	,	"+\
        "	dltotaltbs	,	"+\
        "	servdelta3bytesdistr	,	"+\
        "	servdelta3ulbytesdistr	,	"+\
        "	servdelta3dlbytesdistr	,	"+\
        "	intfdelta3bytesdistr	,	"+\
        "	intfdelta3ulbytesdistr	,	"+\
        "	intfdelta3dlbytesdistr	,	"+\
        "	bytesgrade	,	"+\
        "	avgbytes	,	"+\
        "	ulbytes	,	"+\
        "	dlbytes	,	"+\
        "	totalbytes	,	"+\
        "	(case when totalcqinum>0 then (1-(cqi0_6/totalcqinum))*100 else NULL end) as cqil7distr	,	"+\
        "	cqi0_6	,	"+\
        "	totalcqinum,	"+ \
        yestoday_time_extra +"  as data_date		"+\
        " from public.task_"+str(taskid.loc[0,"taskid"])+"_lte_cell_sum "

        cur.execute(sql)

        data = cur.fetchall()
        if len(data) == 0:
            pass
        else:
            data = pd.DataFrame(data)
            des = cur.description
            colname=[]
            for item in des:
                colname.append(item[0])
            data.columns=colname
    cur.close()
    conn.close()
    print("Trace Postgres Connection close")
    return data

def postgre_upload(data):
    engine = create_engine('postgresql://expert-system:YJY_exp#exp502@10.1.77.51:5432/expert-system')
    data.to_sql("trace_lte_cell_sum_all",engine,schema="cnio", index=False, if_exists='append',method='multi')
    #output = StringIO()
    # data.to_csv(output, sep='\t', index=False, header=False)
    # output1 = output.getvalue()
    # conn = psycopg2.connect(host="10.1.77.51", user="expert-system", password="YJY_exp#exp502",
    #                         database="expert-system", port="5432",options="-c search_path=cnio")
    # cur = conn.cursor()
    # cur.copy_from(StringIO(output1), "trace_lte_cell_sum_all")
    # conn.commit()
    # cur.close()
    # conn.close()
    print("trace_lte_cell_sum_all sync: success")


if __name__ == '__main__':
    yestoday_time_extra= get_date()
    data = postgre_execute(yestoday_time_extra)
    #data.to_csv("trace_mr_cell.csv")
    if len(data)!=0:
        postgre_upload(data)

