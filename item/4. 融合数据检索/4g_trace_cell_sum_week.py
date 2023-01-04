insert
	into
	searching.trace_lte_cell_sum_week 
        select
	cellname,
	cellindex ,
	pci ,
	earfcn ,
	eci ,
	enbid,
	cellid ,
	celllon ,
	celllat ,
	hbwd ,
	azimuth,
	height ,
	indoor ,
	etilt ,
	mtilt ,
	vendor,
	province,
	city,
	district,
	scene,
	covertype,
	siteavgdist,
	celltype,
	count(indoorleak) as indoorleak,
	count(overshooting) as overshooting,
	count(insuffcover) as insuffcover,
	avg(azimuthcheck) as azimuthcheck,
	count(locationerr) as locationerr,
	max(maxdist) as maxdist,
	min(mindist) as mindist,
	avg(avgdist) as avgdist,
	sum(totalmrsample_all) as totalmrsample_all,
	avg(rsrp_all) as rsrp_all,
	sum(bestsample_all) as bestsample_all,
	avg(bestrsrp_all) as bestrsrp_all,
	sum(servingsample_all) as servingsample_all,
	avg(servingrsrp_all) as servingrsrp_all,
	sum(overlap3sample_all) as overlap3sample_all,
	sum(overlap4sample_all) as overlap4sample_all,
	sum(overshootingsample_all) as overshootingsample_all,
	sum(poorcoveragesample_all) as poorcoveragesample_all,
	sum(servdeltam3sample_all) as servdeltam3sample_all,
	sum(servnotbestsample_all) as servnotbestsample_all,
	sum(servdelta3sample_all) as servdelta3sample_all,
	sum(interferencesample_all) as interferencesample_all,
	sum(interfdelta3sample_all) as interfdelta3sample_all,
	sum(mod3sample_all) as mod3sample_all,
	sum(totalmrsample) as totalmrsample,
	avg(rsrp) as rsrp,
	sum(bestsample) as bestsample,
	avg(bestrsrp) as bestrsrp,
	sum(servingsample) as servingsample,
	avg(servingrsrp) as servingrsrp,
	sum(overshootingsample) as overshootingsample,
	sum(poorcoveragesample) as poorcoveragesample,
	sum(overlap3sample) as overlap3sample,
	sum(overlap4sample) as overlap4sample,
	sum(servdeltam3sample) as servdeltam3sample,
	sum(servnotbestsample) as servnotbestsample,
	sum(servdelta3sample) as servdelta3sample,
	sum(interferencesample) as interferencesample,
	sum(interfdelta3sample) as interfdelta3sample,
	sum(mod3sample) as mod3sample,
	avg(totalgridnum) as totalgridnum,
	avg(servingnum) as servingnum,
	avg(overshootingnum) as overshootingnum,
	avg(poorcoveragenum) as poorcoveragenum,
	avg(overlap3num) as overlap3num,
	avg(overlap4num) as overlap4num,
	avg(servdelta3mnum) as servdelta3mnum,
	avg(servnotbestnum) as servnotbestnum,
	avg(servdelta3num) as servdelta3num,
	avg(interferencenum) as interferencenum,
	avg(interfdelta3num) as interfdelta3num,
	avg(conntotalnum) as conntotalnum,
	sum(connreqnum) as connreqnum,
	sum(connfailnum) as connfailnum,
	sum(conndropnum) as conndropnum,
	sum(noendmsgnum) as noendmsgnum,
	sum(reestreqnum) as reestreqnum,
	sum(reestendnum) as reestendnum,
	sum(redirgunum) as redirgunum,
	sum(redirnrnum) as redirnrnum,
	sum(hooutgunum) as hooutgunum,
	sum(hooutnrnum) as hooutnrnum,
	avg(uecapnrnum) as uecapnrnum,
	avg(nbcellnum) as nbcellnum,
	sum(hoouttotalnum) as hoouttotalnum,
	sum(hooutsuccnum) as hooutsuccnum,
	sum(hooutprepfailnum) as hooutprepfailnum,
	sum(hooutexefailnum) as hooutexefailnum,
	sum(hooutsrcreestnum) as hooutsrcreestnum,
	sum(hooutdestreestnum) as hooutdestreestnum,
	avg(servcellnum) as servcellnum,
	sum(hointotalnum) as hointotalnum,
	sum(hoinsuccnum) as hoinsuccnum,
	sum(hoinprepfailnum) as hoinprepfailnum,
	sum(hoinexefailnum) as hoinexefailnum,
	sum(hoinsrcreestnum) as hoinsrcreestnum,
	sum(hoindestreestnum) as hoindestreestnum,
	avg(pciconfservnum) as pciconfservnum,
	sum(pciconfhoouttotalnum) as pciconfhoouttotalnum,
	sum(pciconfhooutsuccnum) as pciconfhooutsuccnum,
	sum(pciconfhooutprepfailnum) as pciconfhooutprepfailnum,
	sum(pciconfhooutexefailnum) as pciconfhooutexefailnum,
	sum(pciconfhooutsrcreestnum) as pciconfhooutsrcreestnum,
	sum(pciconfhooutdestreestnum) as pciconfhooutdestreestnum,
	avg(pciconfdestnum) as pciconfdestnum,
	sum(pciconfhointotalnum) as pciconfhointotalnum,
	sum(pciconfhoinsuccnum) as pciconfhoinsuccnum,
	sum(pciconfhoinprepfailnum) as pciconfhoinprepfailnum,
	sum(pciconfhointexefailnum) as pciconfhointexefailnum,
	sum(pciconfhoinsrcreestnum) as pciconfhoinsrcreestnum,
	sum(pciconfhoindestreestnum) as pciconfhoindestreestnum,
	avg(nbcfgcellnum) as nbcfgcellnum,
	avg(interfercellnum) as interfercellnum,
	avg(mode3cellnum) as mode3cellnum,
	(case when SUM(ultotalmodtbs)=0 then -1 else SUM(ul16qam)* 1.0 / SUM(ultotalmodtbs) end)* 100 as ul16qamdistr,
	(case when SUM(dltotalmodtbs)=0 then -1 else SUM(dl64qam)* 1.0 / SUM(dltotalmodtbs) end)* 100 as dl64qamdistr,
	sum(ul16qam) as ul16qam,
	sum(ultotalmodtbs) as ultotalmodtbs,
	sum(dl64qam) as dl64qam,
	sum(dltotalmodtbs) as dltotalmodtbs,
	(case when SUM(ultotalprb)=0 then -1 else SUM(uldrbprb)* 1.0 / SUM(ultotalprb)* 100 end) as uldrbprbusage,
	(case when SUM(dltotalprb)=0 then -1 else SUM(dldrbprb)* 1.0 / SUM(dltotalprb)* 100 end) as dldrbprbusage,
	sum(uldrbprb) as uldrbprb,
	sum(dldrbprb) as dldrbprb,
	sum(ultotalprb) as ultotalprb,
	sum(dltotalprb) as dltotalprb,
	(case when SUM(ultotaltbs)=0 then -1 else SUM(ulretxtb)* 1.0 / SUM(ultotaltbs)* 100 end) as ulretxrate,
	(case when sum(dltotaltbs)=0 then -1 else SUM(dlretxrate)* 1.0 / sum(dltotaltbs)* 100 end) as dlretxrate,
	sum(ulretxtb) as ulretxtb,
	sum(dlretxtb) as dlretxtb,
	sum(ultotaltbs) as ultotaltbs,
	sum(dltotaltbs) as dltotaltbs,
	avg(servdelta3bytesdistr) as servdelta3bytesdistr,
	avg(servdelta3ulbytesdistr) as servdelta3ulbytesdistr,
	avg(servdelta3dlbytesdistr) as servdelta3dlbytesdistr,
	avg(intfdelta3bytesdistr) as intfdelta3bytesdistr,
	avg(intfdelta3ulbytesdistr) as intfdelta3ulbytesdistr,
	avg(intfdelta3dlbytesdistr) as intfdelta3dlbytesdistr,
	avg(bytesgrade) as bytesgrade,
	avg(avgbytes) as avgbytes,
	sum(ulbytes) as ulbytes,
	sum(dlbytes) as dlbytes,
	sum(totalbytes) as totalbytes,
	(case when sum(totalcqinum)=0 then -1 else sum(cqi0_6)/ sum(totalcqinum)* 100 end) as cqil7distr,
	sum(cqi0_6) as cqi0_6,
	sum(totalcqinum) as totalcqinum,
	${sun} as data_date
from
	(
	select
		*
	from
		searching.trace_lte_cell_sum_day
	where
		data_date=${mon} or 
        data_date=${tues} or 
        data_date=${wednes} or 
        data_date=${thurs} or 
        data_date=${fri} or 
        data_date=${satur} or 
        data_date=${sun}    ) a
group by
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
	celltype;