import pandas as pd
import psycopg2
from sqlalchemy import create_engine


def create_table(id):
    """
    :param id:
    :return: sql
    """
    sql_lte_cell_sum = "drop table if exists public.task_" + id + "_lte_cell_sum;" + \
                       "CREATE TABLE public.task_" + id + "_lte_cell_sum (cellname text NULL," + \
                       "cellindex int8 NULL,	pci int4 NULL,	earfcn int4 NULL,	eci int4 NULL,	enbid int4 NULL,	cellid int4 NULL,	celllon numeric NULL," + \
                       "celllat numeric NULL,	hbwd int4 NULL,	azimuth int4 NULL,	height int4 NULL,	indoor int4 NULL,	etilt int4 NULL," + \
                       "mtilt int4 NULL,	vendor text NULL,	province text NULL,	city text NULL,	district text NULL," + \
                       "scene text NULL,	covertype text NULL,	siteavgdist int4 NULL,	celltype int4 NULL,	indoorleak int4 NULL," + \
                       "overshooting int4 NULL,	insuffcover int4 NULL,	azimuthcheck int4 NULL,	locationerr int4 NULL,	maxdist numeric NULL," + \
                       "mindist numeric NULL,	avgdist numeric NULL,	totalmrsample_all int8 NULL,	rsrp_all float8 NULL," + \
                       "bestsample_all int8 NULL,	bestrsrp_all float8 NULL,	servingsample_all int8 NULL," + \
                       "servingrsrp_all float8 NULL,	overlap3sample_all int8 NULL,	overlap4sample_all int8 NULL," + \
                       "overshootingsample_all int8 NULL,	poorcoveragesample_all int8 NULL,	servdeltam3sample_all int8 NULL," + \
                       "servnotbestsample_all int8 NULL,	servdelta3sample_all int8 NULL,	interferencesample_all int8 NULL," + \
                       "interfdelta3sample_all int8 NULL,	mod3sample_all int8 NULL,	totalmrsample numeric NULL," + \
                       "rsrp float8 NULL,	bestsample numeric NULL,	bestrsrp float8 NULL,	servingsample numeric NULL," + \
                       "servingrsrp float8 NULL,	overshootingsample numeric NULL,	poorcoveragesample numeric NULL,	overlap3sample numeric NULL," + \
                       "overlap4sample numeric NULL,	servdeltam3sample numeric NULL,	servnotbestsample numeric NULL," + \
                       "servdelta3sample numeric NULL,	interferencesample numeric NULL,	interfdelta3sample numeric NULL," + \
                       "mod3sample numeric NULL,	totalgridnum int8 NULL,	servingnum int8 NULL,	overshootingnum int8 NULL," + \
                       "poorcoveragenum int8 NULL,	overlap3num int8 NULL,	overlap4num int8 NULL,	servdelta3mnum int8 NULL," + \
                       "servnotbestnum int8 NULL,	servdelta3num int8 NULL,	interferencenum int8 NULL,	interfdelta3num int8 NULL," + \
                       "conntotalnum int8 NULL,	connreqnum int8 NULL,	connfailnum int8 NULL,	conndropnum int8 NULL," + \
                       "noendmsgnum int8 NULL,	reestreqnum int8 NULL,	reestendnum int8 NULL,	redirgunum int8 NULL," + \
                       "redirnrnum int8 NULL,	hooutgunum int8 NULL,	hooutnrnum int8 NULL,	uecapnrnum int8 NULL,	nbcellnum int8 NULL," + \
                       "hoouttotalnum numeric NULL,	hooutsuccnum numeric NULL,	hooutprepfailnum numeric NULL,	hooutexefailnum numeric NULL," + \
                       "hooutsrcreestnum numeric NULL,	hooutdestreestnum numeric NULL,	servcellnum int8 NULL," + \
                       "hointotalnum numeric NULL,	hoinsuccnum numeric NULL,	hoinprepfailnum numeric NULL," + \
                       "hoinexefailnum numeric NULL,	hoinsrcreestnum numeric NULL,	hoindestreestnum numeric NULL,	pciconfservnum int8 NULL," + \
                       "pciconfhoouttotalnum numeric NULL,	pciconfhooutsuccnum numeric NULL,	pciconfhooutprepfailnum numeric NULL," + \
                       "pciconfhooutexefailnum numeric NULL,	pciconfhooutsrcreestnum numeric NULL,	pciconfhooutdestreestnum numeric NULL," + \
                       "pciconfdestnum int8 NULL,	pciconfhointotalnum numeric NULL,	pciconfhoinsuccnum numeric NULL," + \
                       "pciconfhoinprepfailnum numeric NULL,	pciconfhointexefailnum numeric NULL," + \
                       "pciconfhoinsrcreestnum numeric NULL,	pciconfhoindestreestnum numeric NULL," + \
                       "nbcfgcellnum int8 NULL,	interfercellnum int8 NULL,	mode3cellnum int8 NULL,	ul16qamdistr numeric NULL," + \
                       "dl64qamdistr numeric NULL,	ul16qam numeric NULL,	ultotalmodtbs numeric NULL," + \
                       "dl64qam numeric NULL,	dltotalmodtbs numeric NULL,	uldrbprbusage numeric NULL,	dldrbprbusage numeric NULL," + \
                       "uldrbprb numeric NULL,	dldrbprb numeric NULL,	ultotalprb numeric NULL,	dltotalprb numeric NULL," + \
                       "ulretxrate numeric NULL,	dlretxrate numeric NULL,	ulretxtb numeric NULL,	dlretxtb numeric NULL," + \
                       "ultotaltbs numeric NULL,	dltotaltbs numeric NULL,	servdelta3bytesdistr numeric NULL," + \
                       "servdelta3ulbytesdistr numeric NULL,	servdelta3dlbytesdistr numeric NULL,	intfdelta3bytesdistr numeric NULL," + \
                       "intfdelta3ulbytesdistr numeric NULL,	intfdelta3dlbytesdistr numeric NULL,	bytesgrade int4 NULL," + \
                       "avgbytes numeric NULL,	ulbytes numeric NULL,	dlbytes numeric NULL,	totalbytes numeric NULL," + \
                       "cqil7distr numeric NULL,	cqi0_6 numeric NULL,	totalcqinum numeric NULL);" + \
                       "CREATE INDEX task_" + id + "_lte_cell_sum_cell_idx ON public.task_" + id + "_lte_cell_sum USING btree (cellindex);" + \
                       "CREATE INDEX task_" + id + "_lte_cell_sum_eci_idx ON public.task_" + id + "_lte_cell_sum USING btree (eci);"

    sql_lte_conn_cell_servtype = "drop table if exists public.task_" + id + "_lte_conn_cell_servtype;" + \
                                 "CREATE TABLE public.task_" + id + "_lte_conn_cell_servtype (" + \
                                 "statid int8 NULL,	statperiod int4 NULL,	starttime int8 NULL,	endtime int8 NULL," + \
                                 "cellindex int8 NULL,	enbid int4 NULL,	cellid int4 NULL,	servtype int4 NULL," + \
                                 "totalnum int4 NULL,	setupreqnum int4 NULL,	failnum int4 NULL,	dropnum int4 NULL," + \
                                 "succnum int4 NULL,	noendmsgnum int4 NULL,	reestnum int4 NULL,	hoinnum int4 NULL,	hoinfailnum int4 NULL," + \
                                 "hoinreestnum int4 NULL,	reestendnum int4 NULL,	hoouttotalnum int4 NULL,	hooutfailnum int4 NULL," + \
                                 "hooutsrcreestnum int4 NULL,	hooutdestreestnum int4 NULL,	redirgunum int4 NULL," + \
                                 "redirnrnum int4 NULL,	hooutgunum int4 NULL,	hooutnrnum int4 NULL,	uecapnrnum int4 NULL);"

    sql_lte_conn = "drop table if exists public.task_" + id + "_lte_conn;" + \
                   "CREATE TABLE public.task_" + id + "_lte_conn (" + \
                   "connid int8 NULL,	callid int8 NULL,	servicetype int4 NULL,	connesttype int4 NULL," + \
                   "connstatus int4 NULL,	endcause int4 NULL,	starttime int8 NULL,	endtime int8 NULL,	lon int4 NULL," + \
                   "lat int4 NULL,	gridx int4 NULL,	gridy int4 NULL,	mmeuserid int8 NULL,	srcmmeuserid int8 NULL," + \
                   "enbues1apid int8 NULL,	msisdn int8 NULL,	m_tmsi int8 NULL,	crnti int4 NULL,	enbid int4 NULL," + \
                   "cellid int4 NULL,	destenbid int4 NULL,	destcellid int4 NULL,	uecapnr int4 NULL," + \
                   "redirrat int4 NULL,	hodelay int4 NULL,	hoouttotalnum int4 NULL,	hooutfailnum int4 NULL," + \
                   "fileid int8 NULL);" + \
                   "CREATE INDEX task_" + id + "_lte_conn_cell_idx ON public.task_" + id + "_lte_conn USING btree (enbid, cellid, destenbid, destcellid);" + \
                   "CREATE INDEX task_" + id + "_lte_conn_connid_idx ON public.task_" + id + "_lte_conn USING btree (connid);"

    sql_lte_ho_cell = "drop table if exists public.task_" + id + "_lte_ho_cell;" + \
                      "CREATE TABLE public.task_" + id + "_lte_ho_cell(" + \
                      "eci int4 NULL,nbcellnum int8 NULL, servenbid int4 NULL, servcellid int4 NULL, hoouttotalnum numeric NULL," + \
                      "hooutsuccnum numeric NULL, hooutprepfailnum numeric NULL, hooutexefailnum numeric NULL," + \
                      "hooutsrcreestnum numeric NULL, hooutdestreestnum numeric NULL, hointotalnum numeric NULL," + \
                      "hoinsuccnum numeric NULL, hoinprepfailnum numeric NULL, hoinexefailnum numeric NULL," + \
                      "hoinsrcreestnum numeric NULL, hoindestreestnum numeric NULL, pciconfservnum int8 NULL, " + \
                      "pciconfhoouttotalnum numeric NULL, pciconfhooutsuccnum numeric NULL, pciconfhooutprepfailnum numeric NULL," + \
                      "pciconfhooutexefailnum numeric NULL, pciconfhooutsrcreestnum numeric NULL," + \
                      "pciconfhooutdestreestnum numeric NULL, servcellnum int8 NULL, pciconfdestnum int8 NULL," + \
                      "pciconfhointotalnum numeric NULL, pciconfhoinsuccnum numeric NULL," + \
                      "pciconfhoinprepfailnum numeric NULL, pciconfhointexefailnum numeric NULL," + \
                      "pciconfhoindestreestnum numeric NULL, pciconfhoinsrcreestnum numeric NULL);"

    sql_lte_ho_snbcell = "drop table if exists public.task_" + id + "_lte_ho_snbcell;" + \
                         "CREATE TABLE public.task_" + id + "_lte_ho_snbcell (" + \
                         "serveci int4 NULL,	servenbid int4 NULL,	servcellid int4 NULL,	desteci int4 NULL," + \
                         "destenbid int4 NULL,	destcellid int4 NULL,	destearfcn int4 NULL," + \
                         "destpci int4 NULL,	hoouttotalnum int8 NULL,	hooutsuccnum int8 NULL," + \
                         "hooutprepfailnum int8 NULL,	hooutexefailnum int8 NULL,	hooutsrcreestnum int8 NULL," + \
                         "hooutdestreestnum int8 NULL,	hointotalnum int8 NULL,	hoinsuccnum int8 NULL," + \
                         "hoinprepfailnum int8 NULL,	hoinexefailnum int8 NULL,	hoindestreestnum int8 NULL," + \
                         "hoinsrcreestnum int8 NULL,	intercelldist int4 NULL,	pciconfcellnum int8 NULL," + \
                         "pciconfservid int8 NULL,	pciconfdestid int8 NULL,	pciconfservnum int8 NULL," + \
                         "pciconfdestnum int8 NULL,	pciconfminhonum int8 NULL,	pciconfsumhonum numeric NULL," + \
                         "pciconfsumdestreestnum numeric NULL,	pciconfsumsrcreestnum numeric NULL);"

    sql_lte_hoout_snbcell = "drop table if exists public.task_" + id + "_lte_hoout_snbcell;" + \
                            "CREATE TABLE public.task_" + id + "_lte_hoout_snbcell (" + \
                            "starttime int8 NULL,	endtime int8 NULL,	statid int8 NULL,	statperiod int4 NULL," + \
                            "cellindex int8 NULL,	enbid int4 NULL,	cellid int4 NULL,	destcellindex int8 NULL," + \
                            "destenbid int4 NULL,	destcellid int4 NULL,	destearfcn int4 NULL,	destpci int4 NULL," + \
                            "hototalnum int4 NULL,	s1hosuccnum int4 NULL,	x2hosuccnum int4 NULL,	intraenbhosuccnum int4 NULL," + \
                            "s1hoprepfailnum int4 NULL,	s1hocancelnum int4 NULL,	x2hofailnum int4 NULL,	x2hoprepfailnum int4 NULL," + \
                            "x2hocancelnum int4 NULL,	intraenbhofailnum int4 NULL,	srcreestnum int4 NULL," + \
                            "destreestnum int4 NULL,	intercelldist int4 NULL);"

    sql_lte_mr_cell = "drop table if exists public.task_" + id + "_lte_mr_cell;" + \
                      "CREATE TABLE public.task_" + id + "_lte_mr_cell (" + \
                      "starttime int8 NULL,	endtime int8 NULL,	eci int4 NULL,	cellindex int8 NULL,	pci int4 NULL," + \
                      "earfcn int4 NULL,	totalsample numeric NULL,	rsrp float8 NULL,	bestsample numeric NULL," + \
                      "bestrsrp float8 NULL,	servingsample numeric NULL,	servingrsrp float8 NULL,	servrsrpdistr0 numeric NULL," + \
                      "servrsrpdistr1 numeric NULL,	servrsrpdistr2 numeric NULL,	servrsrpdistr3 numeric NULL," + \
                      "servrsrpdistr4 numeric NULL,	servrsrpdistr5 numeric NULL,	servrsrpdistr6 numeric NULL," + \
                      "servrsrpdistr7 numeric NULL,	servrsrpdistr8 numeric NULL,	servrsrpdistr9 numeric NULL," + \
                      "overlap3sample numeric NULL,	overlap4sample numeric NULL,	nb1sample numeric NULL," + \
                      "nb2sample numeric NULL,	nb3sample numeric NULL,	nb4sample numeric NULL," + \
                      "nb4msample numeric NULL,	overshootingsample numeric NULL,	servovershootingsample numeric NULL," + \
                      "poorcoveragesample numeric NULL,	servdeltam3sample numeric NULL,	servnotbestsample numeric NULL," + \
                      "servdelta3sample numeric NULL,	interferencesample numeric NULL,	interfdelta3sample numeric NULL," + \
                      "mod3sample numeric NULL,	mod6sample numeric NULL,	mod30sample numeric NULL," + \
                      "totalgridnum int8 NULL,	servingnum int8 NULL,	bestnum int8 NULL,	overshootingnum int8 NULL," + \
                      "poorcoveragenum int8 NULL,	servdelta3mnum int8 NULL,	servnotbestnum int8 NULL," + \
                      "servdelta3num int8 NULL,	interferencenum int8 NULL,	interfdelta3num int8 NULL," + \
                      "overlap3num int8 NULL,	overlap4num int8 NULL,	maxservdist numeric NULL," + \
                      "minservdist numeric NULL,	avgservdist numeric NULL,	maxdist numeric NULL," + \
                      "mindist numeric NULL,	avgdist numeric NULL,	azmtotalgridnum numeric NULL," + \
                      "azmhbwd1xnum numeric NULL,	azmhbwd2xnum numeric NULL,	azmhbwd3xnum numeric NULL,	azmphbwd2xnum numeric NULL," + \
                      "azmmhbwd2xnum numeric NULL,	azmrevscellindex int8 NULL,	azmrevsgridnum numeric NULL,mdirxnum int8 NULL," + \
                      "azmmaxdirxnum int8 NULL,	azmmaxdirid int8 NULL);"

    sql_lte_mr_cell_total = "drop table if exists public.task_" + id + "_lte_mr_cell_total;" + \
                            "CREATE TABLE public.task_" + id + "_lte_mr_cell_total (" + \
                            "starttime int8 NULL,	endtime int8 NULL,	eci int4 NULL,	cellindex int8 NULL,	pci int4 NULL," + \
                            "earfcn int4 NULL,	totalsample int8 NULL,	rsrp float8 NULL,	bestsample int8 NULL," + \
                            "bestrsrp float8 NULL,	servingsample int8 NULL,	servingrsrp float8 NULL," + \
                            "servrsrpdistr0 int8 NULL,	servrsrpdistr1 int8 NULL,	servrsrpdistr2 int8 NULL," + \
                            "servrsrpdistr3 int8 NULL,	servrsrpdistr4 int8 NULL,	servrsrpdistr5 int8 NULL," + \
                            "servrsrpdistr6 int8 NULL,	servrsrpdistr7 int8 NULL,	servrsrpdistr8 int8 NULL," + \
                            "servrsrpdistr9 int8 NULL,	nb1sample int8 NULL,	nb2sample int8 NULL," + \
                            "nb3sample int8 NULL,	nb4sample int8 NULL,	nb4msample int8 NULL," + \
                            "overlap3sample int8 NULL,	overlap4sample int8 NULL,	overshootingsample int8 NULL," + \
                            "poorcoveragesample int8 NULL,	servdeltam3sample int8 NULL,	servnotbestsample int8 NULL," + \
                            "servdelta3sample int8 NULL,	interferencesample int8 NULL,	interfdelta3sample int8 NULL," + \
                            "mod3sample int8 NULL,	mod6sample int8 NULL,	mod30sample int8 NULL);"

    sql_lte_mr_grid = "drop table if exists public.task_" + id + "_lte_mr_grid;" + \
                      "CREATE TABLE public.task_" + id + "_lte_mr_grid (" + \
                      "starttime int8 NULL,	endtime int8 NULL,	gridx int4 NULL,	gridy int4 NULL," + \
                      "lat int4 NULL,	lon int4 NULL,	earfcn int4 NULL,	totalsample numeric NULL," + \
                      "servingrsrp float8 NULL,	bestrsrp float8 NULL,	rsrp float8 NULL,	overlap3sample numeric NULL," + \
                      "overlap4sample numeric NULL,	overshootingsample numeric NULL,	poorcoveragesample numeric NULL," + \
                      "servdeltam3sample numeric NULL,	servnotbestsample numeric NULL,	servdelta3sample numeric NULL," + \
                      "interferencesample numeric NULL,	interfdelta3sample numeric NULL,	servrsrpdistr0 numeric NULL," + \
                      "servrsrpdistr1 numeric NULL,	servrsrpdistr2 numeric NULL,	servrsrpdistr3 numeric NULL," + \
                      "servrsrpdistr4 numeric NULL,	servrsrpdistr5 numeric NULL,	servrsrpdistr6 numeric NULL," + \
                      "servrsrpdistr7 numeric NULL,	servrsrpdistr8 numeric NULL,	servrsrpdistr9 numeric NULL," + \
                      "mod3sample numeric NULL,	mod6sample numeric NULL,	mod30sample numeric NULL," + \
                      "totalcellnum int8 NULL,	servcellnum int8 NULL,	bestcellnum int8 NULL," + \
                      "poorcoveragecellnum int8 NULL,	interfercellnum int8 NULL,	maxbestsample int8 NULL," + \
                      "maxservingsample int8 NULL,	top2servcellsample numeric NULL);"

    sql_lte_mr_grid_cell = "drop table if exists public.task_" + id + "_lte_mr_grid_cell;" + \
                           "CREATE TABLE public.task_" + id + "_lte_mr_grid_cell (" + \
                           "starttime int8 NULL,	endtime int8 NULL,	gridx int4 NULL,	gridy int4 NULL," + \
                           "lat int4 NULL,	lon int4 NULL,	eci int4 NULL,	cellindex int8 NULL," + \
                           "pci int4 NULL,	earfcn int4 NULL,	totalsample int8 NULL,	gridtotalsample numeric NULL," + \
                           "gridfreqsample numeric NULL,	rsrp float8 NULL,	bestsample int8 NULL," + \
                           "bestrsrp float8 NULL,	servingsample int8 NULL,	servingrsrp float8 NULL," + \
                           "servrsrpdistr0 int8 NULL,	servrsrpdistr1 int8 NULL,	servrsrpdistr2 int8 NULL," + \
                           "servrsrpdistr3 int8 NULL,	servrsrpdistr4 int8 NULL,	servrsrpdistr5 int8 NULL," + \
                           "servrsrpdistr6 int8 NULL,	servrsrpdistr7 int8 NULL,	servrsrpdistr8 int8 NULL," + \
                           "servrsrpdistr9 int8 NULL,	nb1sample int8 NULL,	nb2sample int8 NULL," + \
                           "nb3sample int8 NULL,	nb4sample int8 NULL,	nb4msample int8 NULL," + \
                           "overlap3sample int8 NULL,	overlap4sample int8 NULL,	servoverlap3sample int8 NULL," + \
                           "servoverlap4sample int8 NULL,	overshootingsample int8 NULL,	servovershootingsample int8 NULL," + \
                           "poorcoveragesample int8 NULL,	servpoorcoveragesample int8 NULL,	servdeltam3sample int8 NULL," + \
                           "servnotbestsample int8 NULL,	servdelta3sample int8 NULL,	interferencesample int8 NULL," + \
                           "servinterferencesample int8 NULL,	interfdelta3sample int8 NULL,	servinterfdelta3sample int8 NULL," + \
                           "mod3sample int8 NULL,	mod6sample int8 NULL,	mod30sample int8 NULL," + \
                           "azmrevscellindex int8 NULL,	dist numeric NULL,	direct int4 NULL);"

    sql_lte_mr_grid_interfcell = "drop table if exists public.task_" + id + "_lte_mr_grid_interfcell;" + \
                                 "CREATE TABLE public.task_" + id + "_lte_mr_grid_interfcell (" + \
                                 "starttime int8 NULL,	endtime int8 NULL,	gridx int4 NULL,	gridy int4 NULL," + \
                                 "lat numeric NULL,	lon numeric NULL,	earfcn int4 NULL,	cellindexa int8 NULL," + \
                                 "pcia int4 NULL,	ecia int4 NULL,	celldista numeric NULL,	celldirecta int4 NULL," + \
                                 "rsrpa float8 NULL,	deltarsrp float8 NULL,	cellindexb int8 NULL,	pcib int4 NULL," + \
                                 "ecib int4 NULL,	celldistb numeric NULL,	celldirectb int4 NULL,	rsrpb float8 NULL," + \
                                 "gridtotalsample numeric NULL,	freqtotalsample numeric NULL,	totalsample numeric NULL," + \
                                 "gridinterfersample numeric NULL,	freqinterfersample numeric NULL,	interferencesample numeric NULL," + \
                                 "intercelldist int4 NULL);"

    sql_lte_mr_grid_snbcell = "drop table if exists public.task_" + id + "_lte_mr_grid_snbcell;" + \
                              "CREATE TABLE public.task_" + id + "_lte_mr_grid_snbcell (" + \
                              "starttime int8 NULL,	endtime int8 NULL,	gridx int4 NULL,	gridy int4 NULL,	lat int4 NULL," + \
                              "lon int4 NULL,	sncellid int8 NULL,	scellindex int8 NULL,	scellearfcn int4 NULL," + \
                              "scellpci int4 NULL,	scelleci int4 NULL,	scelldist numeric NULL,	scelldirect int4 NULL," + \
                              "scellrsrp float8 NULL,	deltarsrp float8 NULL,	gridtotalsample numeric NULL," + \
                              "freqtotalsample numeric NULL,	totalsample int8 NULL,	gridinterfersample numeric NULL," + \
                              "freqinterfersample numeric NULL,	interferencesample int8 NULL,	interfdelta3sample int8 NULL," + \
                              "servdeltadistr0 int8 NULL,	servdeltadistr1 int8 NULL,	servdeltadistr2 int8 NULL," + \
                              "servdeltadistr3 int8 NULL,	servdeltadistr4 int8 NULL,	servdeltadistr5 int8 NULL," + \
                              "servdeltadistr6 int8 NULL,	servdeltadistr7 int8 NULL,	servdeltadistr255 int8 NULL," + \
                              "nb1sample int8 NULL,	nb2sample int8 NULL,	nb3sample int8 NULL,	nb4sample int8 NULL," + \
                              "nb4msample int8 NULL,	ncellindex int8 NULL,	ncellearfcn int4 NULL," + \
                              "ncellpci int4 NULL,	ncelleci int4 NULL,	ncelldist numeric NULL,	ncelldirect int4 NULL," + \
                              "ncellrsrp float8 NULL,	intercelldist int4 NULL);"

    sql_lte_mr_grid_snbcell_total = "drop table if exists public.task_" + id + "_lte_mr_grid_snbcell_total;" + \
                                    "CREATE TABLE public.task_" + id + "_lte_mr_grid_snbcell_total (" + \
                                    "starttime int8 NULL,	endtime int8 NULL,	lon int4 NULL,	lat int4 NULL,	gridx int4 NULL," + \
                                    "gridy int4 NULL,	coverid int8 NULL,	mrtype int4 NULL,	sncellid int8 NULL," + \
                                    "minrsrpth int4 NULL,	maxrsrpth int4 NULL,	mindeltath int4 NULL,	maxdeltath int4 NULL," + \
                                    "scellindex int8 NULL,	scellearfcn int4 NULL,	scellpci int4 NULL,	scelleci int4 NULL," + \
                                    "scelldist int4 NULL,	scelldirect int4 NULL,	scelltype int4 NULL," + \
                                    "scellrsrp float4 NULL,	deltarsrp float4 NULL,	totalsample int4 NULL," + \
                                    "servrsrp float4 NULL,	servsample int4 NULL,	scelloverlap3sample int4 NULL," + \
                                    "scelloverlap4sample int4 NULL,	scellovershootingsample int4 NULL,	scellazmrevscellindex int8 NULL," + \
                                    "ncellindex int8 NULL,	ncellearfcn int4 NULL,	ncellpci int4 NULL," + \
                                    "ncelleci int4 NULL,	ncelldist int4 NULL,	ncelldirect int4 NULL,	ncelltype int4 NULL," + \
                                    "ncellrsrp float4 NULL,	ncelloverlap3sample int4 NULL,	ncelloverlap4sample int4 NULL," + \
                                    "ncellovershootingsample int4 NULL,	ncellazmrevscellindex int8 NULL,	nb1sample int4 NULL," + \
                                    "nb1rsrp float4 NULL,	nb2sample int4 NULL,	nb2rsrp float4 NULL,	nb3sample int4 NULL," + \
                                    "nb3rsrp float4 NULL,	nb4sample int4 NULL,	nb4rsrp float4 NULL,	nb4msample int4 NULL," + \
                                    "nb4mrsrp float4 NULL,	intercelldist int4 NULL);"

    sql_lte_mr_overlaparea = "drop table if exists public.task_" + id + "_lte_mr_overlaparea;" + \
                             "CREATE TABLE public.task_" + id + "_lte_mr_overlaparea (" + \
                             "areaid int4 NULL,	rowid int8 NULL,	starttime int8 NULL,	endtime int8 NULL,	gridx int4 NULL," + \
                             "gridy int4 NULL,	lat int4 NULL,	lon int4 NULL,	areaprop int4 NULL,	earfcn int4 NULL," + \
                             "servingrsrp float8 NULL,	bestrsrp float8 NULL,	totalsample numeric NULL,	overlap3sample numeric NULL," + \
                             "overlap4sample numeric NULL);"

    sql_lte_mr_poorcoveragearea = "drop table if exists public.task_" + id + "_lte_mr_poorcoveragearea;" + \
                                  "CREATE TABLE public.task_" + id + "_lte_mr_poorcoveragearea (" + \
                                  "areaid int4 NULL,	rowid int8 NULL,	areaprop int4 NULL,	starttime int8 NULL,	endtime int8 NULL," + \
                                  "lon int4 NULL,	lat int4 NULL,	gridx int4 NULL,	gridy int4 NULL,	freqnum int8 NULL," + \
                                  "servingrsrp float8 NULL,	bestrsrp float8 NULL,	totalsample numeric NULL," + \
                                  "poorcoveragesample numeric NULL);"

    sql_lte_mr_snbcell = "drop table if exists public.task_" + id + "_lte_mr_snbcell;" + \
                         "CREATE TABLE public.task_" + id + "_lte_mr_snbcell (" + \
                         "starttime int8 NULL,	endtime int8 NULL,	sncellid int8 NULL,	scellindex int8 NULL," + \
                         "scellearfcn int4 NULL,	scellpci int4 NULL,	scelleci int4 NULL,	scellmaxdist numeric NULL," + \
                         "scellavgdist numeric NULL,	scellrsrp float8 NULL,	deltarsrp float8 NULL," + \
                         "totalsample numeric NULL,	interferencesample numeric NULL,	interfdelta3sample numeric NULL," + \
                         "servdeltadistr0 numeric NULL,	servdeltadistr1 numeric NULL,	servdeltadistr2 numeric NULL," + \
                         "servdeltadistr3 numeric NULL,	servdeltadistr4 numeric NULL,	servdeltadistr5 numeric NULL," + \
                         "servdeltadistr6 numeric NULL,	servdeltadistr7 numeric NULL,	servdeltadistr255 numeric NULL," + \
                         "nb1sample numeric NULL,	nb2sample numeric NULL,	nb3sample numeric NULL," + \
                         "nb4sample numeric NULL,	nb4msample numeric NULL,	totalgridnum int8 NULL," + \
                         "interferencenum int8 NULL,	interfdelta3num int8 NULL,	servdeltam3num int8 NULL," + \
                         "servnotbestnum int8 NULL,	servdelta3num int8 NULL,	ncellindex int8 NULL," + \
                         "ncellearfcn int4 NULL,	ncellpci int4 NULL,	ncelleci int4 NULL,	ncellmaxdist numeric NULL," + \
                         "ncellavgdist numeric NULL,	ncellrsrp float8 NULL,	intercelldist int4 NULL);"

    sql_lte_mr_snbcell_total = "drop table if exists public.task_" + id + "_lte_mr_snbcell_total;" + \
                               "CREATE TABLE public.task_" + id + "_lte_mr_snbcell_total (" + \
                               "starttime int8 NULL,	endtime int8 NULL,	sncellid int8 NULL,	scellindex int8 NULL," + \
                               "scellearfcn int4 NULL,	scellpci int4 NULL,	scelleci int4 NULL,	scellrsrp float8 NULL," + \
                               "deltarsrp float8 NULL,	totalsample int8 NULL,	interferencesample int8 NULL," + \
                               "interfdelta3sample int8 NULL,	servdeltadistr0 int8 NULL,	servdeltadistr1 int8 NULL," + \
                               "servdeltadistr2 int8 NULL,	servdeltadistr3 int8 NULL,	servdeltadistr4 int8 NULL," + \
                               "servdeltadistr5 int8 NULL,	servdeltadistr6 int8 NULL,	servdeltadistr7 int8 NULL," + \
                               "servdeltadistr255 int8 NULL,	nb1sample int8 NULL,	nb2sample int8 NULL," + \
                               "nb3sample int8 NULL,	nb4sample int8 NULL,	nb4msample int8 NULL," + \
                               "ncellindex int8 NULL,	ncellearfcn int4 NULL,	ncellpci int4 NULL," + \
                               "ncelleci int4 NULL,	ncellrsrp float8 NULL,	intercelldist int4 NULL);"

    sql_lte_mr_user = "drop table if exists public.task_" + id + "_lte_mr_user;" + \
                      "CREATE TABLE public.task_" + id + "_lte_mr_user (" + \
                      "connid int8 NULL,	mrtime int8 NULL,	fileid int8 NULL,	servicetype int4 NULL," + \
                      "servicegrade int4 NULL,	msisdn int8 NULL,	mmeuserid int8 NULL,	enbid int4 NULL," + \
                      "cellid int8 NULL,	crnti int8 NULL,	lat int4 NULL,	lon int4 NULL," + \
                      "gridx int4 NULL,	gridy int4 NULL,	eventid int4 NULL,	pci int4 NULL," + \
                      "earfcn int4 NULL,	rsrp float4 NULL,	rsrq float4 NULL,	scellsortno int4 NULL," + \
                      "nbnum int4 NULL,	nb1pci int4 NULL,	nb1rsrp float4 NULL," + \
                      "nb2pci int4 NULL,	nb2rsrp float4 NULL,	nb3pci int4 NULL," + \
                      "nb3rsrp float4 NULL,	interfreqearfcn int4 NULL,	interfreqnbpci int4 NULL," + \
                      "interfreqnbrsrp float4 NULL,	deltarsrpsn1 float4 NULL,	poorcoverage int4 NULL," + \
                      "overlapcellnum int4 NULL);"

    sql_lte_mr_userconn = "drop table if exists public.task_" + id + "_lte_mr_userconn;" + \
                          "CREATE TABLE public.task_" + id + "_lte_mr_userconn (" + \
                          "connid int8 NULL,	starttime int8 NULL,	endtime int8 NULL,	lat int4 NULL," + \
                          "lon int4 NULL,	gridx int4 NULL,	gridy int4 NULL,	fileid int8 NULL," + \
                          "servicetype int4 NULL,	servicegrade int4 NULL,	msisdn int8 NULL,	mmeuserid int8 NULL," + \
                          "crnti int4 NULL,	enbid int4 NULL,	cellid int4 NULL,	earfcn int4 NULL," + \
                          "pci int4 NULL,	servrsrp float4 NULL,	totalsample int4 NULL,	servnotbestsample int4 NULL," + \
                          "poorcoveragesample int4 NULL,	overlap3sample int4 NULL,	overlap4sample int4 NULL," + \
                          "overshootingsample int4 NULL,	servdeltadistr0 int4 NULL,	servdeltadistr1 int4 NULL," + \
                          "servdeltadistr2 int4 NULL,	servdeltadistr3 int4 NULL,	servdeltadistr4 int4 NULL," + \
                          "servdeltadistr5 int4 NULL,	servdeltadistr6 int4 NULL,	servdeltadistr7 int4 NULL," + \
                          "nb1pci int4 NULL,	nb1rsrp float4 NULL,	nb1deltarsrp float4 NULL," + \
                          "nb1sample int4 NULL,	nb2pci int4 NULL,	nb2rsrp float4 NULL,	nb2deltarsrp float4 NULL," + \
                          "nb2sample int4 NULL,	nb3pci int4 NULL,	nb3rsrp float4 NULL,	nb3deltarsrp float4 NULL," + \
                          "nb3sample int4 NULL,	interfreq int4 NULL,	interfreqnbpci int4 NULL," + \
                          "interfreqnbrsrp float4 NULL);"

    sql_lte_snbcell_sum = "drop table if exists public.task_" + id + "_lte_snbcell_sum;" + \
                          "CREATE TABLE public.task_" + id + "_lte_snbcell_sum (" + \
                          "scellindex int8 NULL,	scellearfcn int4 NULL,	scellpci int4 NULL," + \
                          "scelleci int4 NULL,	ncellindex int8 NULL,	ncellearfcn int4 NULL," + \
                          "ncellpci int4 NULL,	ncelleci int4 NULL,	sncellid int8 NULL," + \
                          "totalmrsample_all int8 NULL,	interferencesample_all int8 NULL," + \
                          "interfdelta3sample_all int8 NULL,	servdeltam6sample_all int8 NULL," + \
                          "servdeltam3sample_all int8 NULL,	servdelta0sample_all int8 NULL,	servdelta3sample_all int8 NULL," + \
                          "totalmrsample numeric NULL,	interferencesample numeric NULL,	interfdelta3sample numeric NULL," + \
                          "servdeltam6sample numeric NULL,	servdeltam3sample numeric NULL," + \
                          "servdelta0sample numeric NULL,	servdelta3sample numeric NULL," + \
                          "totalgridnum int8 NULL,	interferencenum int8 NULL,	interfdelta3num int8 NULL," + \
                          "servdeltam3num int8 NULL,	servnotbestnum int8 NULL,	servdelta3num int8 NULL," + \
                          "hoouttotalnum int8 NULL,	hooutsuccnum int8 NULL,	hooutprepfailnum int8 NULL," + \
                          "hooutexefailnum int8 NULL,	hooutsrcreestnum int8 NULL,	hooutdestreestnum int8 NULL," + \
                          "hointotalnum int8 NULL,	hoinsuccnum int8 NULL,	hoinprepfailnum int8 NULL," + \
                          "hoinexefailnum int8 NULL,	hoinsrcreestnum int8 NULL,	hoindestreestnum int8 NULL," + \
                          "pciconfcellnum int8 NULL,	pciconfservid int8 NULL,	pciconfdestid int8 NULL," + \
                          "pciconfservnum int8 NULL,	pciconfdestnum int8 NULL,	pciconfminhonum int8 NULL," + \
                          "pciconfsumhonum numeric NULL,	pciconfsumsrcreestnum numeric NULL," + \
                          "pciconfsumdestreestnum numeric NULL,	servdelta3ulbytes numeric NULL,	servdelta0ulbytes numeric NULL," + \
                          "servdeltam3ulbytes numeric NULL,	servdelta3dlbytes numeric NULL,	servdelta0dlbytes numeric NULL," + \
                          "servdeltam3dlbytes numeric NULL,	intfdelta3ulbytes numeric NULL,	intfdelta3dlbytes numeric NULL," + \
                          "ulbytes numeric NULL,	dlbytes numeric NULL,	totalbytes numeric NULL,	ulqpsk int8 NULL," + \
                          "ul16qam int8 NULL,	ul64qam int8 NULL,	ultotalmodtbs int8 NULL," + \
                          "dlqpsk int8 NULL,	dl16qam int8 NULL,	dl64qam int8 NULL,	dltotalmodtbs int8 NULL," + \
                          "uldrbprb numeric NULL,	dldrbprb numeric NULL,	ulprb numeric NULL," + \
                          "dlprb numeric NULL,	ultotalprb numeric NULL,	dltotalprb numeric NULL," + \
                          "ulretxtb int8 NULL,	dlretxtb int8 NULL,	ultotaltbs int8 NULL,	dltotaltbs int8 NULL," + \
                          "totaluuextsample int8 NULL,	totalcqinum int8 NULL,	cqi0_6 int8 NULL,	cqi7_10 int8 NULL," + \
                          "cqi11_15 int8 NULL,	intercelldist int4 NULL);"

    sql_lte_uuextend_cell = "drop table if exists public.task_" + id + "_lte_uuextend_cell;" + \
                            "CREATE TABLE public.task_" + id + "_lte_uuextend_cell (" + \
                            "rectime int8 NULL,	fileid int8 NULL,	recordnum int4 NULL,	enbid int4 NULL," + \
                            "cellid int4 NULL,	s1rx int8 NULL,	s1tx int8 NULL,	ul_userplanebytes int8 NULL," + \
                            "dl_userplanebytes int8 NULL,	ul_pdcpbytes int8 NULL,	dl_pdcpbytes int8 NULL," + \
                            "ul_prb_drb int8 NULL,	dl_prb_drb int8 NULL,	ul_prb int8 NULL,	dl_prb int8 NULL," + \
                            "ul_prb_total int8 NULL,	dl_prb_total int8 NULL,	dl_pdcp_sdu_packs_tx_drb int8 NULL," + \
                            "enb_discard_page int8 NULL,	enb_received_page int8 NULL,	ul_mac_throughput int8 NULL," + \
                            "dl_mac_throughput int8 NULL,	ul_tbs_qpsk int8 NULL,	dl_tbs_qpsk int8 NULL,	ul_tbs_16qam int8 NULL," + \
                            "dl_tbs_16qam int8 NULL,	ul_tbs_64qam int8 NULL,	dl_tbs_64qam int8 NULL," + \
                            "ul_mac_tb_total int8 NULL,	dl_mac_tb_total int8 NULL,	ul_harq_tb_retx int8 NULL," + \
                            "dl_harq_tb_retx int8 NULL,	online_user_count int8 NULL,	dl_ol_ri1_schdcount int8 NULL," + \
                            "dl_tm2_schedcount int8 NULL,	dl_tm3_schedcount int8 NULL,	dl_tm7_schedcount int8 NULL," + \
                            "dl_tm8_schedcount int8 NULL,	dl_total_schedcount int8 NULL);"

    sql_lte_uuextend_cellhour = "drop table if exists public.task_" + id + "_lte_uuextend_cellhour;" + \
                                "CREATE TABLE public.task_" + id + "_lte_uuextend_cellhour (" + \
                                "statperiod int4 NULL,	statid int8 NULL,	starttime int8 NULL,	endtime int8 NULL," + \
                                "cellindex int8 NULL,	enbid int4 NULL,	cellid int4 NULL,	recordnum int4 NULL," + \
                                "s1_rx int8 NULL,	s1_tx int8 NULL,	ul_user_plane_bytes int8 NULL,	dl_user_plane_bytes int8 NULL," + \
                                "ul_pdcp_bytes int8 NULL,	dl_pdcp_bytes int8 NULL,	ul_prb_drb int8 NULL," + \
                                "dl_prb_drb int8 NULL,	ul_prb int8 NULL,	dl_prb int8 NULL,	ul_prb_total int8 NULL," + \
                                "dl_prb_total int8 NULL,	dl_pdcp_sdu_packs_lost_drb int8 NULL,	dl_pdcp_sdu_packs_discard_drb int8 NULL," + \
                                "ul_pdcp_sdu_packs_lost_drb int8 NULL,	ul_pdcp_sdu_packs_expected_drb int8 NULL,	dl_pdcp_sdu_packs_tx_drb int8 NULL," + \
                                "enodeb_discard_page int8 NULL,	enodeb_received_page int8 NULL,	ul_mac_throughput_num int8 NULL," + \
                                "ul_mac_throughput_avg int8 NULL,	dl_mac_throughput_num int8 NULL,	dl_mac_throughput_avg int8 NULL," + \
                                "ul_tbs_qpsk int8 NULL,	dl_tbs_qpsk int8 NULL,	ul_tbs_16qam int8 NULL,	dl_tbs_16qam int8 NULL," + \
                                "ul_tbs_64qam int8 NULL,	dl_tbs_64qam int8 NULL,	ul_mac_tb_total int8 NULL," + \
                                "dl_mac_tb_total int8 NULL,	ul_harq_tb_retx int8 NULL,	dl_harq_tb_retx int8 NULL," + \
                                "online_user_count_max int4 NULL,	online_user_count_avg int4 NULL,	online_user_count_num int4 NULL," + \
                                "dl_ol_ri1_schedule_count int8 NULL,	dl_ol_ri2_schedule_count int8 NULL,	dl_tm2_schedule_count int8 NULL," + \
                                "dl_tm3_schedule_count int8 NULL,	dl_tm7_schedule_count int8 NULL,	dl_tm8_schedule_count int8 NULL," + \
                                "dl_total_schedule_count int8 NULL,	ul_pack_lost_qci1 int4 NULL,	dl_pack_lost_qci1 int4 NULL," + \
                                "ul_pack_lost_qci2 int4 NULL,	dl_pack_lost_qci2 int4 NULL,	ul_pack_lost_qci3 int4 NULL," + \
                                "dl_pack_lost_qci3 int4 NULL,	ul_pack_lost_qci4 int4 NULL,	dl_pack_lost_qci4 int4 NULL," + \
                                "ul_pack_lost_qci5 int4 NULL,	dl_pack_lost_qci5 int4 NULL,	ul_pack_lost_qci6 int4 NULL," + \
                                "dl_pack_lost_qci6 int4 NULL,	ul_pack_lost_qci7 int4 NULL,	dl_pack_lost_qci7 int4 NULL," + \
                                "ul_pack_lost_qci8 int4 NULL,	dl_pack_lost_qci8 int4 NULL);"

    sql_lte_uuextend_user = "drop table if exists public.task_" + id + "_lte_uuextend_user;" + \
                            "CREATE TABLE public.task_" + id + "_lte_uuextend_user (" + \
                            "connid int8 NULL,	rectime int8 NULL,	fileid int8 NULL,	servicetype int4 NULL,	servicegrade int4 NULL," + \
                            "msisdn int8 NULL,	mmeuserid int8 NULL,	enbid int4 NULL,	cellid int4 NULL,	crnti int4 NULL," + \
                            "recordnum int4 NULL,	phr int8 NULL,	ul_sinr int8 NULL,	enb_rssi int8 NULL,	ta int8 NULL," + \
                            "recvintf int8 NULL,	aoa int8 NULL,	ul_userplanebytes int8 NULL,	dl_userplanebytes int8 NULL," + \
                            "ul_pdcpbytes int8 NULL,	dl_pdcpbytes int8 NULL,	ul_prb_drb int8 NULL,	dl_prb_drb int8 NULL," + \
                            "ul_prb int8 NULL,	dl_prb int8 NULL,	ul_prb_total int8 NULL,	dl_prb_total int8 NULL," + \
                            "dl_pdcp_sdu_packs_lost_drb int8 NULL,	dl_pdcp_sdu_packs_discard_drb int8 NULL," + \
                            "ul_pdcp_sdu_packs_lost_drb int8 NULL,	ul_pdcp_sdu_packs_expected_drb int8 NULL," + \
                            "dl_pdcp_sdu_packs_tx_drb int8 NULL,	ul_mac_throughput int8 NULL,	dl_mac_throughput int8 NULL," + \
                            "ul_tbs_qpsk int8 NULL,	dl_tbs_qpsk int8 NULL,	ul_tbs_16qam int8 NULL,	dl_tbs_16qam int8 NULL," + \
                            "ul_tbs_64qam int8 NULL,	dl_tbs_64qam int8 NULL,	ul_mac_tb_total int8 NULL,	dl_mac_tb_total int8 NULL," + \
                            "ul_harq_tb_retx int8 NULL,	dl_harq_tb_retx int8 NULL,	pdcplastfragacktime int8 NULL," + \
                            "pdcprecvdatafromupperlayertime int8 NULL,	cqi int8 NULL,	pmi int8 NULL,	ri int8 NULL);"

    sql_lte_uuextend_user_cell = "drop table if exists public.task_" + id + "_lte_uuextend_user_cell;" + \
                                 "CREATE TABLE public.task_" + id + "_lte_uuextend_user_cell (" + \
                                 "cellindex int8 NULL,	eci int4 NULL,	avgbytes numeric NULL,	bytesid int8 NULL,	totalcellnum int8 NULL," + \
                                 "servdelta3ulbytes numeric NULL,	servdelta0ulbytes numeric NULL,	servdeltam3ulbytes numeric NULL," + \
                                 "intfdelta3ulbytes numeric NULL,	servdelta3dlbytes numeric NULL,	servdelta0dlbytes numeric NULL," + \
                                 "servdeltam3dlbytes numeric NULL,	intfdelta3dlbytes numeric NULL,	ulbytes numeric NULL," + \
                                 "dlbytes numeric NULL,	totalbytes numeric NULL,	ulqpsk numeric NULL,	ul16qam numeric NULL," + \
                                 "ul64qam numeric NULL,	ultotalmodtbs numeric NULL,	dlqpsk numeric NULL,	dl16qam numeric NULL,	" + \
                                 "dl64qam numeric NULL,	dltotalmodtbs numeric NULL,	uldrbprb numeric NULL,	dldrbprb numeric NULL," + \
                                 "ulprb numeric NULL,	dlprb numeric NULL,	ultotalprb numeric NULL," + \
                                 "dltotalprb numeric NULL,	ulretxtb numeric NULL,	dlretxtb numeric NULL," + \
                                 "ultotaltbs numeric NULL,	dltotaltbs numeric NULL,	totalsample numeric NULL,	servdelta3sample numeric NULL," + \
                                 "servdelta0sample numeric NULL,	servdeltam3sample numeric NULL,	totalcqinum numeric NULL," + \
                                 "cqi0_6 numeric NULL,	cqi7_10 numeric NULL,	cqi11_15 numeric NULL,	filenum int8 NULL," + \
                                 "servdelta3bytesdistr numeric NULL,	servdelta0bytesdistr numeric NULL," + \
                                 "servdeltam3bytesdistr numeric NULL,	intfdelta3bytesdistr numeric NULL,	servdelta3ulbytesdistr numeric NULL," + \
                                 "servdelta0ulbytesdistr numeric NULL,	servdeltam3ulbytesdistr numeric NULL,	intfdelta3ulbytesdistr numeric NULL," + \
                                 "servdelta3dlbytesdistr numeric NULL,	servdelta0dlbytesdistr numeric NULL,	servdeltam3dlbytesdistr numeric NULL," + \
                                 "intfdelta3dlbytesdistr numeric NULL,	ulqpskdistr numeric NULL,	ul16qamdistr numeric NULL,	ul64qamdistr numeric NULL," + \
                                 "dlqpskdistr numeric NULL,	dl16qamdistr numeric NULL,	dl64qamdistr numeric NULL," + \
                                 "ulretxrate numeric NULL,	dlretxrate numeric NULL,	uldrbprbusage numeric NULL," + \
                                 "dldrbprbusage numeric NULL,	cqil7distr numeric NULL,	cqil11distr numeric NULL,	bytesgrade int4 NULL);"

    sql_lte_uuextend_user_grid_snbcell_total = "drop table if exists public.task_" + id + "_lte_uuextend_user_grid_snbcell_total;" + \
                                               "CREATE TABLE public.task_" + id + "_lte_uuextend_user_grid_snbcell_total (" + \
                                               "starttime int8 NULL,	endtime int8 NULL,	lon int4 NULL,	lat int4 NULL," + \
                                               "gridx int4 NULL,	gridy int4 NULL,	sncellid int8 NULL,	scellpci int4 NULL," + \
                                               "scellearfcn int4 NULL,	scelleci int4 NULL,	scellindex int8 NULL,	scelltype int4 NULL," + \
                                               "ncellpci int4 NULL,	ncellearfcn int4 NULL,	ncelleci int4 NULL,	ncellindex int8 NULL," + \
                                               "ncelltype int4 NULL,	coverid int4 NULL,	recordnum int4 NULL,	phrnum int4 NULL," + \
                                               "phrdistr0 int4 NULL,	phrdistr1 int4 NULL,	phrdistr2 int4 NULL,	phrdistr3 int4 NULL,	phrdistr4 int4 NULL," + \
                                               "phrdistr5 int4 NULL,	ul_sinr_avg float4 NULL,	ul_sinr_num int4 NULL," + \
                                               "ul_sinrdistr0 int4 NULL,	ul_sinrdistr1 int4 NULL,	ul_sinrdistr2 int4 NULL," + \
                                               "ul_sinrdistr3 int4 NULL,	ul_sinrdistr4 int4 NULL,	ul_sinrdistr5 int4 NULL," + \
                                               "ul_sinrdistr6 int4 NULL,	ul_sinrdistr7 int4 NULL,	rssi_avg float4 NULL," + \
                                               "rssi_num int4 NULL,	ta_avg int4 NULL,	ta_num int4 NULL,	tadistr0 int4 NULL,	tadistr1 int4 NULL," + \
                                               "tadistr2 int4 NULL,	tadistr3 int4 NULL,	tadistr4 int4 NULL,	tadistr5 int4 NULL,	tadistr6 int4 NULL," + \
                                               "tadistr7 int4 NULL,	tadistr8 int4 NULL,	tadistr9 int4 NULL,	rip_avg float4 NULL," + \
                                               "rip_num int4 NULL,	aoa_avg int4 NULL,	aoa_num int4 NULL,	ul_user_plane_bytes int8 NULL," + \
                                               "dl_user_plane_bytes int8 NULL,	ul_pdcp_bytes int8 NULL,	dl_pdcp_bytes int8 NULL,	ul_prb_drb int8 NULL," + \
                                               "dl_prb_drb int8 NULL,	ul_prb int8 NULL,	dl_prb int8 NULL,	ul_prb_total int8 NULL," + \
                                               "dl_prb_total int8 NULL,	dl_pdcp_sdu_packs_lost_drb int8 NULL,	dl_pdcp_sdu_packs_discard_drb int8 NULL," + \
                                               "ul_pdcp_sdu_packs_lost_drb int8 NULL,	ul_pdcp_sdu_packs_expected_drb int8 NULL," + \
                                               "dl_pdcp_sdu_packs_tx_drb int8 NULL,	ul_mac_throughput_avg int4 NULL,	ul_mac_throughput_num int4 NULL," + \
                                               "ul_mac_throughputdistr0 int4 NULL,	ul_mac_throughputdistr1 int4 NULL,	ul_mac_throughputdistr2 int4 NULL," + \
                                               "ul_mac_throughputdistr3 int4 NULL,	ul_mac_throughputdistr4 int4 NULL,	ul_mac_throughputdistr5 int4 NULL," + \
                                               "ul_mac_throughputdistr6 int4 NULL,	ul_mac_throughputdistr7 int4 NULL,	dl_mac_throughput_avg int4 NULL," + \
                                               "dl_mac_throughput_num int4 NULL,	dl_mac_throughputdistr0 int4 NULL,	dl_mac_throughputdistr1 int4 NULL," + \
                                               "dl_mac_throughputdistr2 int4 NULL,	dl_mac_throughputdistr3 int4 NULL,	dl_mac_throughputdistr4 int4 NULL," + \
                                               "dl_mac_throughputdistr5 int4 NULL,	dl_mac_throughputdistr6 int4 NULL,	dl_mac_throughputdistr7 int4 NULL," + \
                                               "ul_tbs_qpsk int4 NULL,	dl_tbs_qpsk int4 NULL,	ul_tbs_16qam int4 NULL,	dl_tbs_16qam int4 NULL," + \
                                               "ul_tbs_64qam int4 NULL,	dl_tbs_64qam int4 NULL,	ul_mac_tb_total int4 NULL,	dl_mac_tb_total int4 NULL," + \
                                               "ul_harq_tb_retx int4 NULL,	dl_harq_tb_retx int4 NULL,	cqinum int4 NULL,	cqidistr0 int4 NULL," + \
                                               "cqidistr1 int4 NULL,	cqidistr2 int4 NULL,	rinum int4 NULL,	ri2num int4 NULL);"

    sql_lte_uuextend_user_snbcell = "drop table if exists public.task_" + id + "_lte_uuextend_user_snbcell;" + \
                                    "CREATE TABLE public.task_" + id + "_lte_uuextend_user_snbcell (" + \
                                    "scellindex int8 NULL,	scelleci int4 NULL,	ncellindex int8 NULL,	ncelleci int4 NULL," + \
                                    "ncellearfcn int4 NULL,	ncellpci int4 NULL,	sncellid int8 NULL,	servdelta3ulbytes numeric NULL," + \
                                    "servdelta0ulbytes numeric NULL,	servdeltam3ulbytes numeric NULL,	intfdelta3ulbytes numeric NULL," + \
                                    "servdelta3dlbytes numeric NULL,	servdelta0dlbytes numeric NULL,	servdeltam3dlbytes numeric NULL," + \
                                    "intfdelta3dlbytes numeric NULL,	ulbytes numeric NULL,	dlbytes numeric NULL,	totalbytes numeric NULL," + \
                                    "ulqpsk int8 NULL,	ul16qam int8 NULL,	ul64qam int8 NULL,	ultotalmodtbs int8 NULL,	dlqpsk int8 NULL," + \
                                    "dl16qam int8 NULL,	dl64qam int8 NULL,	dltotalmodtbs int8 NULL,	uldrbprb numeric NULL,	dldrbprb numeric NULL," + \
                                    "ulprb numeric NULL,	dlprb numeric NULL,	ultotalprb numeric NULL,	dltotalprb numeric NULL," + \
                                    "ulretxtb int8 NULL,	dlretxtb int8 NULL,	ultotaltbs int8 NULL,	dltotaltbs int8 NULL,	totalsample int8 NULL," + \
                                    "servdelta3sample int8 NULL,	servdelta0sample int8 NULL,	servdeltam3sample int8 NULL,	intfdelta3sample int8 NULL," + \
                                    "totalcqinum int8 NULL,	cqi0_6 int8 NULL,	cqi7_10 int8 NULL,	cqi11_15 int8 NULL);"

    sql_lte_uuextend_userconn = "drop table if exists public.task_" + id + "_lte_uuextend_userconn;" + \
                                "CREATE TABLE public.task_" + id + "_lte_uuextend_userconn (" + \
                                "connid int8 NULL,	starttime int8 NULL,	endtime int8 NULL,	fileid int8 NULL,	servicetype int4 NULL," + \
                                "servicegrade int4 NULL,	msisdn int8 NULL,	mmeuserid int8 NULL,	crnti int4 NULL," + \
                                "enbid int4 NULL,	cellid int4 NULL,	earfcn int4 NULL,	pci int4 NULL," + \
                                "recordnum int4 NULL,	phrnum int4 NULL,	phrdistr0 int4 NULL,	phrdistr1 int4 NULL," + \
                                "phrdistr2 int4 NULL,	phrdistr3 int4 NULL,	phrdistr4 int4 NULL,	phrdistr5 int4 NULL," + \
                                "ul_sinr_avg float4 NULL,	ul_sinr_num int4 NULL,	ul_sinrdistr0 int4 NULL,	ul_sinrdistr1 int4 NULL," + \
                                "ul_sinrdistr2 int4 NULL,	ul_sinrdistr3 int4 NULL,	ul_sinrdistr4 int4 NULL,	ul_sinrdistr5 int4 NULL," + \
                                "ul_sinrdistr6 int4 NULL,	ul_sinrdistr7 int4 NULL,	rssi_avg float4 NULL,	rssi_num int4 NULL," + \
                                "ta_avg int4 NULL,	ta_num int4 NULL,	tadistr0 int4 NULL,	tadistr1 int4 NULL,	tadistr2 int4 NULL," + \
                                "tadistr3 int4 NULL,	tadistr4 int4 NULL,	tadistr5 int4 NULL,	tadistr6 int4 NULL,	tadistr7 int4 NULL," + \
                                "tadistr8 int4 NULL,	tadistr9 int4 NULL,	rip_avg float4 NULL,	rip_num int4 NULL,	aoa_avg int4 NULL," + \
                                "aoa_num int4 NULL,	ul_user_plane_bytes int8 NULL,	dl_user_plane_bytes int8 NULL," + \
                                "ul_pdcp_bytes int8 NULL,	dl_pdcp_bytes int8 NULL,	ul_prb_drb int8 NULL,	dl_prb_drb int8 NULL," + \
                                "ul_prb int8 NULL,	dl_prb int8 NULL,	ul_prb_total int8 NULL,	dl_prb_total int8 NULL," + \
                                "dl_pdcp_sdu_packs_lost_drb int8 NULL,	dl_pdcp_sdu_packs_discard_drb int8 NULL," + \
                                "ul_pdcp_sdu_packs_lost_drb int8 NULL,	ul_pdcp_sdu_packs_expected_drb int8 NULL," + \
                                "dl_pdcp_sdu_packs_tx_drb int8 NULL,	ul_mac_throughput_avg int4 NULL,	ul_mac_throughput_num int4 NULL," + \
                                "ul_mac_throughputdistr0 int4 NULL,	ul_mac_throughputdistr1 int4 NULL,	ul_mac_throughputdistr2 int4 NULL," + \
                                "ul_mac_throughputdistr3 int4 NULL,	ul_mac_throughputdistr4 int4 NULL,	ul_mac_throughputdistr5 int4 NULL," + \
                                "ul_mac_throughputdistr6 int4 NULL,	ul_mac_throughputdistr7 int4 NULL,	dl_mac_throughput_avg int4 NULL," + \
                                "dl_mac_throughput_num int4 NULL,	dl_mac_throughputdistr0 int4 NULL,	dl_mac_throughputdistr1 int4 NULL," + \
                                "dl_mac_throughputdistr2 int4 NULL,	dl_mac_throughputdistr3 int4 NULL,	dl_mac_throughputdistr4 int4 NULL," + \
                                "dl_mac_throughputdistr5 int4 NULL,	dl_mac_throughputdistr6 int4 NULL,	dl_mac_throughputdistr7 int4 NULL," + \
                                "ul_tbs_qpsk int4 NULL,	dl_tbs_qpsk int4 NULL,	ul_tbs_16qam int4 NULL,	dl_tbs_16qam int4 NULL," + \
                                "ul_tbs_64qam int4 NULL,	dl_tbs_64qam int4 NULL,	ul_mac_tb_total int4 NULL,	dl_mac_tb_total int4 NULL," + \
                                "ul_harq_tb_retx int4 NULL,	dl_harq_tb_retx int4 NULL,	cqinum int4 NULL,	cqidistr0 int4 NULL," + \
                                "cqidistr1 int4 NULL,	cqidistr2 int4 NULL,	rinum int4 NULL,	ri2num int4 NULL);"

    sql_siteinfo = "drop table if exists public.task_" + id + "_siteinfo;" + \
                   "CREATE TABLE public.task_" + id + "_siteinfo (" + \
                   "celltype int4 NULL,	cellindex int8 NULL,	ciindex int4 NULL,	rattype int8 NULL,	channel int8 NULL," + \
                   "pci int4 NULL,	cellname text NULL,	lon int4 NULL,	lat int4 NULL,	azimuth int4 NULL,	hbwd int4 NULL," + \
                   "siteid int4 NULL,	sitename text NULL,	cellid int4 NULL,	ci int4 NULL," + \
                   "tac int4 NULL,	band int4 NULL,	poweron int4 NULL,	indoor int4 NULL,	vbwd int4 NULL," + \
                   "height int4 NULL,	etilt int4 NULL,	mtilt int4 NULL,	vendor text NULL,	province text NULL," + \
                   "city text NULL,	district text NULL,	scene text NULL,	covertype text NULL,	sversion int4 NULL," + \
                   "maxversion int4 NULL,	sitedensity int4 NULL,	siteavgdist int4 NULL,	scene_no int4 NULL);"

    sql = sql_lte_cell_sum + sql_lte_conn_cell_servtype + sql_lte_conn + sql_lte_ho_cell + sql_lte_ho_snbcell + \
          sql_lte_hoout_snbcell + sql_lte_mr_cell + sql_lte_mr_cell_total + sql_lte_mr_grid + sql_lte_mr_grid_cell + \
          sql_lte_mr_grid_interfcell + sql_lte_mr_grid_snbcell + sql_lte_mr_grid_snbcell_total + sql_lte_mr_overlaparea + \
          sql_lte_mr_poorcoveragearea + sql_lte_mr_snbcell + sql_lte_mr_snbcell_total + sql_lte_mr_userconn + sql_lte_snbcell_sum + \
          sql_lte_uuextend_cell + sql_lte_uuextend_cellhour + sql_lte_uuextend_user + sql_lte_uuextend_user_cell + \
          sql_lte_uuextend_user_grid_snbcell_total + sql_lte_uuextend_user_snbcell + sql_lte_uuextend_userconn + sql_siteinfo

    return sql

def insert_table(id):
    last_id = int(id)-1
    sql_lte_cell_sum = "insert into public.task_"+id+"_lte_cell_sum select * from public.task_"+\
                       str(last_id)+"_lte_cell_sum;"
    sql_lte_conn_cell_servtype = "insert into public.task_"+id+"_lte_conn_cell_servtype select * from public.task_"+\
                       str(last_id)+"_lte_conn_cell_servtype;"
    sql_lte_conn = "insert into public.task_"+id+"_lte_conn select * from public.task_"+\
                       str(last_id)+"_lte_conn;"
    sql_lte_ho_cell= "insert into public.task_"+id+"_lte_ho_cell select * from public.task_"+\
                       str(last_id)+"_lte_ho_cell;"
    sql_lte_ho_snbcell= "insert into public.task_"+id+"_lte_ho_snbcell select * from public.task_"+\
                       str(last_id)+"_lte_ho_snbcell;"
    sql_lte_hoout_snbcell = "insert into public.task_"+id+"_lte_hoout_snbcell select * from public.task_"+\
                       str(last_id)+"_lte_hoout_snbcell;"
    sql_lte_mr_cell = "insert into public.task_"+id+"_lte_mr_cell select * from public.task_"+\
                       str(last_id)+"_lte_mr_cell;"
    sql_lte_mr_cell_total = "insert into public.task_"+id+"_lte_mr_cell_total select * from public.task_"+\
                       str(last_id)+"_lte_mr_cell_total;"
    sql_lte_mr_grid = "insert into public.task_"+id+"_lte_mr_grid select * from public.task_"+\
                       str(last_id)+"_lte_mr_grid;"
    sql_lte_mr_grid_cell = "insert into public.task_"+id+"_lte_mr_grid_cell select * from public.task_"+\
                       str(last_id)+"_lte_mr_grid_cell;"
    sql_lte_mr_grid_interfcell = "insert into public.task_"+id+"_lte_mr_grid_interfcell select * from public.task_"+\
                       str(last_id)+"_lte_mr_grid_interfcell;"
    sql_lte_mr_grid_snbcell= "insert into public.task_"+id+"_lte_mr_grid_snbcell select * from public.task_"+\
                       str(last_id)+"_lte_mr_grid_snbcell;"
    sql_lte_mr_grid_snbcell_total = "insert into public.task_"+id+"_lte_mr_grid_snbcell_total select * from public.task_"+\
                       str(last_id)+"_lte_mr_grid_snbcell_total;"
    sql_lte_mr_overlaparea = "insert into public.task_"+id+"_lte_mr_overlaparea select * from public.task_"+\
                       str(last_id)+"_lte_mr_overlaparea;"
    sql_lte_mr_poorcoveragearea = "insert into public.task_"+id+"_lte_mr_poorcoveragearea select * from public.task_"+\
                       str(last_id)+"_lte_mr_poorcoveragearea;"
    sql_lte_mr_snbcell = "insert into public.task_"+id+"_lte_mr_snbcell select * from public.task_"+\
                       str(last_id)+"_lte_mr_snbcell;"
    sql_lte_mr_snbcell_total = "insert into public.task_"+id+"_lte_mr_snbcell_total select * from public.task_"+\
                       str(last_id)+"_lte_mr_snbcell_total;"
    sql_lte_mr_userconn = "insert into public.task_"+id+"_lte_mr_userconn select * from public.task_"+\
                       str(last_id)+"_lte_mr_userconn;"
    sql_lte_snbcell_sum = "insert into public.task_"+id+"_lte_snbcell_sum select * from public.task_"+\
                       str(last_id)+"_lte_snbcell_sum;"
    sql_lte_uuextend_cell= "insert into public.task_"+id+"_lte_uuextend_cell select * from public.task_"+\
                       str(last_id)+"_lte_uuextend_cell;"
    sql_lte_uuextend_cellhour = "insert into public.task_"+id+"_lte_uuextend_cellhour  select * from public.task_"+\
                       str(last_id)+"_lte_uuextend_cellhour ;"
    sql_lte_uuextend_user = "insert into public.task_"+id+"_lte_uuextend_user select * from public.task_"+\
                       str(last_id)+"_lte_uuextend_user;"
    sql_lte_uuextend_user_cell = "insert into public.task_"+id+"_lte_uuextend_user_cell select * from public.task_"+\
                       str(last_id)+"_lte_uuextend_user_cell;"
    sql_lte_uuextend_user_grid_snbcell_total  = "insert into public.task_"+id+"_lte_uuextend_user_grid_snbcell_total  select * from public.task_"+\
                       str(last_id)+"_lte_uuextend_user_grid_snbcell_total ;"
    sql_lte_uuextend_user_snbcell = "insert into public.task_"+id+"_lte_uuextend_user_snbcell select * from public.task_"+\
                       str(last_id)+"_lte_uuextend_user_snbcell;"
    sql_lte_uuextend_userconn = "insert into public.task_"+id+"_lte_uuextend_userconn select * from public.task_"+\
                       str(last_id)+"_lte_uuextend_userconn;"
    sql_siteinfo = "insert into public.task_"+id+"_siteinfo select * from public.task_"+\
                       str(last_id)+"_siteinfo;"


    sql = sql_lte_cell_sum + sql_lte_conn_cell_servtype + sql_lte_conn + sql_lte_ho_cell + sql_lte_ho_snbcell + \
          sql_lte_hoout_snbcell + sql_lte_mr_cell + sql_lte_mr_cell_total + sql_lte_mr_grid + sql_lte_mr_grid_cell + \
          sql_lte_mr_grid_interfcell + sql_lte_mr_grid_snbcell + sql_lte_mr_grid_snbcell_total + sql_lte_mr_overlaparea + \
          sql_lte_mr_poorcoveragearea + sql_lte_mr_snbcell + sql_lte_mr_snbcell_total + sql_lte_mr_userconn + sql_lte_snbcell_sum + \
          sql_lte_uuextend_cell + sql_lte_uuextend_cellhour + sql_lte_uuextend_user + sql_lte_uuextend_user_cell + \
          sql_lte_uuextend_user_grid_snbcell_total + sql_lte_uuextend_user_snbcell + sql_lte_uuextend_userconn + sql_siteinfo

    return sql

def update_table(id):
    update_lte_conn_cell_servtype = "update public.task_"+id+"_lte_conn_cell_servtype set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8); update public.task_"+id+"_lte_conn_cell_servtype set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8);"
    update_lte_conn = "update public.task_"+id+"_lte_conn set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8); update public.task_"+id+"_lte_conn set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8);"
        #update_lte_ho_cell = "update public.task_"+id+"_lte_ho_cell set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8); update public.task_"+id+"_lte_ho_cell set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8);"
        #update_lte_ho_snbcell = "update public.task_"+id+"_lte_ho_snbcell set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8); update public.task_"+id+"_lte_ho_snbcell set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8);"
    update_lte_hoout_snbcell = "update public.task_"+id+"_lte_hoout_snbcell set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8); update public.task_"+id+"_lte_hoout_snbcell set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8);"
    update_lte_mr_cell = "update public.task_"+id+"_lte_mr_cell set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8); update public.task_"+id+"_lte_mr_cell set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8);"
    update_lte_mr_cell_total = "update public.task_"+id+"_lte_mr_cell_total set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8); update public.task_"+id+"_lte_mr_cell_total set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8);"
    update_lte_mr_grid = "update public.task_"+id+"_lte_mr_grid set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8); update public.task_"+id+"_lte_mr_grid set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8);"
    update_lte_mr_grid_cell = "update public.task_"+id+"_lte_mr_grid_cell set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8); update public.task_"+id+"_lte_mr_grid_cell set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8);"
    update_lte_mr_grid_interfcell = "update public.task_"+id+"_lte_mr_grid_interfcell set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8); update public.task_"+id+"_lte_mr_grid_interfcell set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8);"
    update_lte_mr_grid_snbcell = "update public.task_"+id+"_lte_mr_grid_snbcell set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8); update public.task_"+id+"_lte_mr_grid_snbcell set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8);"
    update_lte_mr_grid_snbcell_total = "update public.task_"+id+"_lte_mr_grid_snbcell_total set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8); update public.task_"+id+"_lte_mr_grid_snbcell_total set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8);"
    update_lte_mr_overlaparea = "update public.task_"+id+"_lte_mr_overlaparea set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8); update public.task_"+id+"_lte_mr_overlaparea set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8);"
    update_lte_mr_poorcoveragearea = "update public.task_"+id+"_lte_mr_poorcoveragearea set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8); update public.task_"+id+"_lte_mr_poorcoveragearea set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8);"
    update_lte_mr_snbcell = "update public.task_"+id+"_lte_mr_snbcell set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8); update public.task_"+id+"_lte_mr_snbcell set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8);"
    update_lte_mr_snbcell_total = "update public.task_"+id+"_lte_mr_snbcell_total set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8); update public.task_"+id+"_lte_mr_snbcell_total set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8);"
    update_lte_mr_userconn = "update public.task_"+id+"_lte_mr_userconn set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8); update public.task_"+id+"_lte_mr_userconn set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8);"
        #update_lte_snbcell_sum = "update public.task_"+id+"_lte_snbcell_sum set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8);update public.task_"+id+"_lte_snbcell_sum set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8); "
    update_lte_uuextend_cell = "update public.task_"+id+"_lte_uuextend_cell set rectime =  cast(extract(epoch from to_timestamp(rectime  / 1000) - interval '1 day') * 1000 as int8); "
    update_lte_uuextend_cellhour = "update public.task_"+id+"_lte_uuextend_cellhour set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8); update public.task_"+id+"_lte_uuextend_cellhour set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8);"
    update_lte_uuextend_user = "update public.task_"+id+"_lte_uuextend_user set rectime =  cast(extract(epoch from to_timestamp(rectime  / 1000) - interval '1 day') * 1000 as int8); "
   # update_lte_uuextend_user_cell = "update public.task_"+id+"_lte_uuextend_user_cell set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8); update public.task_"+id+"_lte_uuextend_user_cell set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8);"
    update_lte_uuextend_user_grid_snbcell_total = "update public.task_"+id+"_lte_uuextend_user_grid_snbcell_total set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8); update public.task_"+id+"_lte_uuextend_user_grid_snbcell_total set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8);"
    update_lte_uuextend_user_snbcell = "update public.task_"+id+"_lte_uuextend_user_snbcell set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8); update public.task_"+id+"_lte_uuextend_user_snbcell set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8);"
    update_lte_uuextend_userconn = "update public.task_"+id+"_lte_uuextend_userconn set starttime =  cast(extract(epoch from to_timestamp(starttime  / 1000) - interval '1 day') * 1000 as int8); update public.task_"+id+"_lte_uuextend_userconn set endtime =  cast(extract(epoch from to_timestamp(endtime  / 1000) - interval '1 day') * 1000 as int8); "



    sql = update_lte_conn_cell_servtype \
          + update_lte_conn  \
          + update_lte_hoout_snbcell \
          + update_lte_mr_cell + update_lte_mr_cell_total + update_lte_mr_grid + update_lte_mr_grid_cell  \
          + update_lte_mr_grid_interfcell + update_lte_mr_grid_snbcell + update_lte_mr_grid_snbcell_total + update_lte_mr_overlaparea \
          + update_lte_mr_poorcoveragearea \
          + update_lte_mr_snbcell \
          + update_lte_mr_snbcell_total \
          + update_lte_mr_userconn \
          + update_lte_uuextend_cell \
          + update_lte_uuextend_cellhour  \
          + update_lte_uuextend_user \
          + update_lte_uuextend_user_grid_snbcell_total \
          + update_lte_uuextend_userconn
    return sql



def postgre_trace():
    # conn = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502", host="10.1.77.51", port="5432")
    conn = psycopg2.connect(database="trace-tables", user="trace-tables", password="YJY_tra#tra502", host="10.1.77.51",
                            port="5432")
    print("pgsql 连接建立")
    cur = conn.cursor()
    # 执行查询
    sql_trace_sk_task = "select st.taskid,st.file_filter from public.sk_task st where taskid = (select max(taskid) from public.sk_task )"
    # 查询taskid
    cur.execute(sql_trace_sk_task)
    sk_task = cur.fetchall()
    sk_task = pd.DataFrame(sk_task)


    ## trace建表
    # sql_create_table = create_table(str(sk_task.iloc[0, 0]))
    # cur.execute(sql_create_table)
    # conn.commit()
    # print("trace 建表结束")


    # trace表格复制
    sql_insert_table = insert_table(str(sk_task.iloc[0, 0]))
    cur.execute(sql_insert_table)
    conn.commit()

    print("trace 表格复制结束")

    # trace表格修改
    sql_update_table = update_table(str(sk_task.iloc[0, 0]))
    cur.execute(sql_update_table)
    conn.commit()

    print("trace 表格修改结束")

    cur.close()
    conn.close()
    print("pgsql 连接释放")


## print("Connection release")


if __name__ == '__main__':
    postgre_trace()
