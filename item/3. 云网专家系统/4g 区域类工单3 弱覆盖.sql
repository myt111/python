
create table if not exists cnio.overlap_coverage_area_cell(
area_id text,
lat_area text,
lon_area text,
cnt text,
rk text,
gridx text,
gridy text,
lat text,
lon text,
eci text,
cellindex text,
pci text,
earfcn text,
totalsample text,
gridtotalsample text,
gridfreqsample text,
rsrp text,
bestsample text,
bestrsrp text,
azmrevscellindex text,
dist text,
direct text,
trace_date text
);

create table if not exists cnio.overlap_info_area_cell(
areaid integer,
lon numeric,
lat numeric,
gridx integer,
gridy integer,
rattype text,
channel bigint,
pci integer,
cellname text,
azimuth text,
hbwd integer,
sitename text,
cellid integer,
eci integer,
tac integer,
band text,
poweron integer,
indoor integer,
vbwd integer,
height integer,
etilt integer,
mtilt integer,
top_site_id integer,
top_dist numeric,
rankid integer
);  


     
create table if not exists cnio.overlap_cell_weakcoverageall
(areaid                  text ,
lat                      text,
lon                      text,
cnt                      text,
alarm_cell_num           text,
alarm_cell_sum           text,
alarm_ratio              text,
ldist_cell_num           text,
ldist_cell_sum           text,
ldist_cell_ratio         text,
azimuthcheck_cell_num    text,
azimuthcheck_cell_sum    text,
azimuthcheck_cell_ratio  text,
locationerr_cell_num     text,
locationerr_cell_sum     text,
locationerr_cell_ratio   text,
overshooting_cell_num    text,
overshooting_cell_sum    text,
overshooting_cell_ratio  text,
sdist_cell_num           text,
sdist_cell_sum           text,
sdist_cell_ratio         text
);



CREATE TABLE if not exists cnio.task_43_lte_cell_sum (
    cellname text NULL,
    cellindex int8 NULL,
    pci int4 NULL,
    earfcn int8 NULL,
    eci int4 NULL,
    enbid int4 NULL,
    cellid int4 NULL,
    celllon numeric NULL,
    celllat numeric NULL,
    hbwd int4 NULL,
    azimuth int4 NULL,
    height int4 NULL,
    indoor int4 NULL,
    etilt int4 NULL,
    mtilt int4 NULL,
    vendor text NULL,
    province text NULL,
    city text NULL,
    district text NULL,
    scene text NULL,
    covertype text NULL,
    siteavgdist int4 NULL,
    celltype int4 NULL,
    indoorleak int4 NULL,
    overshooting int4 NULL,
    insuffcover int4 NULL,
    azimuthcheck int4 NULL,
    locationerr int4 NULL,
    maxdist numeric NULL,
    mindist numeric NULL,
    avgdist numeric NULL,
    totalmrsample_all int8 NULL,
    rsrp_all float8 NULL,
    bestsample_all int8 NULL,
    bestrsrp_all float8 NULL,
    servingsample_all int8 NULL,
    servingrsrp_all float8 NULL,
    overlap3sample_all int8 NULL,
    overlap4sample_all int8 NULL,
    overshootingsample_all int8 NULL,
    poorcoveragesample_all int8 NULL,
    servdeltam3sample_all int8 NULL,
    servnotbestsample_all int8 NULL,
    servdelta3sample_all int8 NULL,
    interferencesample_all int8 NULL,
    interfdelta3sample_all int8 NULL,
    mod3sample_all int8 NULL,
    totalmrsample numeric NULL,
    rsrp float8 NULL,
    bestsample numeric NULL,
    bestrsrp float8 NULL,
    servingsample numeric NULL,
    servingrsrp float8 NULL,
    overshootingsample numeric NULL,
    poorcoveragesample numeric NULL,
    overlap3sample numeric NULL,
    overlap4sample numeric NULL,
    servdeltam3sample numeric NULL,
    servnotbestsample numeric NULL,
    servdelta3sample numeric NULL,
    interferencesample numeric NULL,
    interfdelta3sample numeric NULL,
    mod3sample numeric NULL,
    totalgridnum int8 NULL,
    servingnum int8 NULL,
    overshootingnum int8 NULL,
    poorcoveragenum int8 NULL,
    overlap3num int8 NULL,
    overlap4num int8 NULL,
    servdelta3mnum int8 NULL,
    servnotbestnum int8 NULL,
    servdelta3num int8 NULL,
    interferencenum int8 NULL,
    interfdelta3num int8 NULL,
    conntotalnum int8 NULL,
    connreqnum int8 NULL,
    connfailnum int8 NULL,
    conndropnum int8 NULL,
    noendmsgnum int8 NULL,
    reestreqnum int8 NULL,
    reestendnum int8 NULL,
    redirgunum int8 NULL,
    redirnrnum int8 NULL,
    hooutgunum int8 NULL,
    hooutnrnum int8 NULL,
    uecapnrnum int8 NULL,
    nbcellnum int8 NULL,
    hoouttotalnum numeric NULL,
    hooutsuccnum numeric NULL,
    hooutprepfailnum numeric NULL,
    hooutexefailnum numeric NULL,
    hooutsrcreestnum numeric NULL,
    hooutdestreestnum numeric NULL,
    servcellnum int8 NULL,
    hointotalnum numeric NULL,
    hoinsuccnum numeric NULL,
    hoinprepfailnum numeric NULL,
    hoinexefailnum numeric NULL,
    hoinsrcreestnum numeric NULL,
    hoindestreestnum numeric NULL,
    pciconfservnum int8 NULL,
    pciconfhoouttotalnum numeric NULL,
    pciconfhooutsuccnum numeric NULL,
    pciconfhooutprepfailnum numeric NULL,
    pciconfhooutexefailnum numeric NULL,
    pciconfhooutsrcreestnum numeric NULL,
    pciconfhooutdestreestnum numeric NULL,
    pciconfdestnum int8 NULL,
    pciconfhointotalnum numeric NULL,
    pciconfhoinsuccnum numeric NULL,
    pciconfhoinprepfailnum numeric NULL,
    pciconfhointexefailnum numeric NULL,
    pciconfhoinsrcreestnum numeric NULL,
    pciconfhoindestreestnum numeric NULL,
    nbcfgcellnum int8 NULL,
    interfercellnum int8 NULL,
    mode3cellnum int8 NULL,
    ul16qamdistr numeric NULL,
    dl64qamdistr numeric NULL,
    ul16qam numeric NULL,
    ultotalmodtbs numeric NULL,
    dl64qam numeric NULL,
    dltotalmodtbs numeric NULL,
    uldrbprbusage numeric NULL,
    dldrbprbusage numeric NULL,
    uldrbprb numeric NULL,
    dldrbprb numeric NULL,
    ultotalprb numeric NULL,
    dltotalprb numeric NULL,
    ulretxrate numeric NULL,
    dlretxrate numeric NULL,
    ulretxtb numeric NULL,
    dlretxtb numeric NULL,
    ultotaltbs numeric NULL,
    dltotaltbs numeric NULL,
    servdelta3bytesdistr numeric NULL,
    servdelta3ulbytesdistr numeric NULL,
    servdelta3dlbytesdistr numeric NULL,
    intfdelta3bytesdistr numeric NULL,
    intfdelta3ulbytesdistr numeric NULL,
    intfdelta3dlbytesdistr numeric NULL,
    bytesgrade int4 NULL,
    avgbytes numeric NULL,
    ulbytes numeric NULL,
    dlbytes numeric NULL,
    totalbytes numeric NULL,
    cqil7distr numeric NULL,
    cqi0_6 numeric NULL,
    totalcqinum numeric NULL
);



    
create table if not exists cnio.overlap_all(
areaid text,
lat_area text,
lon_area text,
grid_num text,
alarm_cell_num text,
alarm_cell_sum text,
alarm_ratio text,
azimuthcheck_cell_num text,
azimuthcheck_cell_sum text,
azimuthcheck_cell_ratio text,
locationerr_cell_num text,
locationerr_cell_sum text,
locationerr_cell_ratio text,
overshooting_cell_num text,
overshooting_cell_sum text,
overshooting_cell_ratio text,
average_dist text,
servingrsrp text,
bestrsrp text,
servingnobest_grid_num text,
order_sorting text,
root_cause text,
tuning_action text,
comments text,
order_date text
);



create table if not exists cnio.poorcoverage_area_cell(
area_id text,
lat_area text,
lon_area text,
cnt text,
rk text,
gridx text,
gridy text,
lat text,
lon text,
eci text,
cellindex text,
pci text,
earfcn text,
totalsample text,
gridtotalsample text,
gridfreqsample text,
rsrp text,
bestsample text,
bestrsrp text,
azmrevscellindex text,
dist text,
direct text,
trace_date text
);         

                
create table if not exists cnio.siteinfo_area_cell(
areaid integer,
lon numeric,
lat numeric,
gridx integer,
gridy integer,
rattype text,
channel bigint,
pci integer,
cellname text,
azimuth text,
hbwd integer,
sitename text,
cellid integer,
eci integer,
tac integer,
band text,
poweron integer,
indoor integer,
vbwd integer,
height integer,
etilt integer,
mtilt integer,
top_site_id integer,
top_dist numeric,
rankid integer
);     


 
create table if not exists cnio.poorcoverage_cell_weakcoverageall
(areaid                  text ,
lat                      text,
lon                      text,
cnt                      text,
alarm_cell_num           text,
alarm_cell_sum           text,
alarm_ratio              text,
ldist_cell_num           text,
ldist_cell_sum           text,
ldist_cell_ratio         text,
azimuthcheck_cell_num    text,
azimuthcheck_cell_sum    text,
azimuthcheck_cell_ratio  text,
locationerr_cell_num     text,
locationerr_cell_sum     text,
locationerr_cell_ratio   text,
overshooting_cell_num    text,
overshooting_cell_sum    text,
overshooting_cell_ratio  text,
sdist_cell_num           text,
sdist_cell_sum           text,
sdist_cell_ratio         text
);




create table if not exists cnio.poorcoverage_area_judgement
(
    areaid                  text,
    lat                     text,
    lon                     text,
    cnt                     text,
    alarm_cell_num          text,
    alarm_cell_sum          text,
    alarm_ratio             text,
    ldist_cell_num          text,
    ldist_cell_sum          text,
    ldist_cell_ratio        text,
    azimuthcheck_cell_num   text,
    azimuthcheck_cell_sum   text,
    azimuthcheck_cell_ratio text,
    locationerr_cell_num    text,
    locationerr_cell_sum    text,
    locationerr_cell_ratio  text,
    overshooting_cell_num   text,
    overshooting_cell_sum   text,
    overshooting_cell_ratio text,
    sdist_cell_num          text,
    sdist_cell_sum          text,
    sdist_cell_ratio        text,
    order_sorting text,
    root_cause text,
    tuning_action text,
    comments text,
    order_date text

);



CREATE TABLE if not exists cnio.task_43_lte_mr_grid_cell (
    starttime int8 NULL,
    endtime int8 NULL,
    gridx int4 NULL,
    gridy int4 NULL,
    lat int4 NULL,
    lon int4 NULL,
    eci int4 NULL,
    cellindex int8 NULL,
    pci int4 NULL,
    earfcn int8 NULL,
    totalsample int8 NULL,
    rsrp float8 NULL,
    bestsample int8 NULL,
    bestrsrp float8 NULL,
    servingsample int8 NULL,
    servingrsrp float8 NULL,
    servrsrpdistr0 int8 NULL,
    servrsrpdistr1 int8 NULL,
    servrsrpdistr2 int8 NULL,
    servrsrpdistr3 int8 NULL,
    servrsrpdistr4 int8 NULL,
    servrsrpdistr5 int8 NULL,
    servrsrpdistr6 int8 NULL,
    servrsrpdistr7 int8 NULL,
    servrsrpdistr8 int8 NULL,
    servrsrpdistr9 int8 NULL,
    nb1sample int8 NULL,
    nb2sample int8 NULL,
    nb3sample int8 NULL,
    nb4sample int8 NULL,
    nb4msample int8 NULL,
    overlap3sample int8 NULL,
    overlap4sample int8 NULL,
    servoverlap3sample int8 NULL,
    servoverlap4sample int8 NULL,
    overshootingsample int8 NULL,
    servovershootingsample int8 NULL,
    poorcoveragesample int8 NULL,
    servpoorcoveragesample int8 NULL,
    servdeltam3sample int8 NULL,
    servnotbestsample int8 NULL,
    servdelta3sample int8 NULL,
    interferencesample int8 NULL,
    servinterferencesample int8 NULL,
    interfdelta3sample int8 NULL,
    servinterfdelta3sample int8 NULL,
    mod3sample int8 NULL,
    mod6sample int8 NULL,
    mod30sample int8 NULL,
    azmrevscellindex int8 NULL,
    dist numeric NULL,
    direct int4 NULL,
    gridfreqsample numeric NULL,
    gridtotalsample numeric NULL
);

               
insert into cnio.poorcoverage_area_cell
select d.area_id,d.lon as lon_area,d.lat as lat_area,d.cnt,row_number() over (partition by area_id order by e.dist ) rk,e.*
from (
       select c.area_id, b.gridx, b.gridy, count(c.area_id) over (partition by c.area_id) cnt, c.lon, c.lat
       from (select a.area_id, a.lon, a.lat
             from (select area_id, avg(lon) as lon, avg(lat) as lat
                   from cnio.trace_fault_grid
             where fault_type = '弱覆盖'
                   group by area_id) a
            ) c
                left join
            cnio.trace_fault_grid  b on b.area_id = c.area_id where fault_type = '弱覆盖'
   ) d
       left join cnio.trace_mr_grid_cell e
                 on d.gridx = e.gridx and d.gridy = e.gridy where trace_date = cast(to_char(now()::timestamp  + '-2 day +8 hour', 'yyyy-mm-dd') as date) ;


                



insert into cnio.siteinfo_area_cell
select e.area_id,
       e.lon1 as lon,
       e.lat1 as lat,
       e.gridx,
       e.gridy,
       e.rattype,
       e.channel,
       e.pci,
       e.cellname,
       e.azimuth,
       e.hbwd,
       e.sitename,
       e.cellid,
       e.ci   as eci,
       e.tac,
       e.band,
       e.poweron,
       e.indoor,
       e.vbwd,
       e.height,
       e.etilt,
       e.mtilt,
       top_site_id,
       top_dist,
       rankid
from (select d.*,
             case when id != 0 then siteid else 0 end   as top_site_id,
             case when id != 0 then distance else 0 end as top_dist,
             id                                         as rankid
      from (
               select c.*,
                      dense_rank() over (partition by c.lon2,c.lat2 order by c.distance ) as id
               from (
                        select x.*,
                               111265 * SQRT(power((lat1 - lat2), 2) +
                                             power(COS((lat1 + lat2) * 3.14159 / 360) * (lon1 - lon2), 2)) /
                               1000.0 as distance
                        from (
                                 select b.siteid,
                                        b.lon  as lon1,
                                        b.lat  as lat1,
                                        c1.lon as lon2,
                                        c1.lat as lat2,
                                        c1.area_id,
                                        c1.gridx,
                                        c1.gridy,

                                        b.rattype,
                                        b.channel,
                                        b.pci,
                                        b.cellname,
                                        b.azimuth,
                                        b.hbwd,
                                        b.sitename,
                                        b.cellid,
                                        b.ci,
                                        b.tac,
                                        b.band,
                                        b.poweron,
                                        b.indoor,
                                        b.vbwd,
                                        b.height,
                                        b.etilt,
                                        b.mtilt
                                 from (select a.lon,
                                              a.lat,
                                              a.area_id,
                                              a.gridx,
                                              a.gridy
                                       from cnio.trace_fault_grid a
                                       where fault_type = '弱覆盖'
                                      ) c1,
                                      (
                                          select distinct siteid,
                                                          lon,
                                                          lat,

                                                          rattype,
                                                          channel,
                                                          pci,
                                                          cellname,
                                                          azimuth,
                                                          hbwd,
                                                          sitename,
                                                          cellid,
                                                          ci,
                                                          tac,
                                                          band,
                                                          poweron,
                                                          indoor,
                                                          vbwd,
                                                          height,
                                                          etilt,
                                                          mtilt
                                          from cnio.siteinfo
                                          where indoor = 0
                                            and rattype = '4G') b
                             ) x) c
           ) d
      where id <= 3) e;

    
              
                
    
insert into cnio.poorcoverage_cell_weakcoverageall
select m1.areaid           as areaid,
       m7.lat_area         as lat,
       m7.lon_area         as lon,
       m7.cnt              as cnt,
       m6.alarmectcnt      as alarm_cell_num,
       m6.eciallcnt        as alarm_cell_sum,
       m6.prec             as alarm_ratio,
       m1.coveragesum      as ldist_cell_num,
       m1.ecicnt           as ldist_cell_sum,
       m1.coverprec        as ldist_cell_ratio,
       m2.sumaz            as azimuthcheck_cell_num,
       m2.ecisum           as azimuthcheck_cell_sum,
       m2.prec             as azimuthcheck_cell_ratio,
       m3.locationerr      as locationerr_cell_num,
       m3.ecisum           as locationerr_cell_sum,
       m3.prec             as locationerr_cell_ratio,
       m4.overshooting     as overshooting_cell_num,
       m4.ecisum           as overshooting_cell_sum,
       m4.prec             as overshooting_cell_ratio,
       m5.sdist_cell_num   as sdist_cell_num,
       m5.sdist_cell_sum   as sdist_cell_sum,
       m5.sdist_cell_ratio as sdist_cell_ratio
from (
         select t4.areaid,
                t4.coveragesum,
                t5.ecicnt,
                cast(t4.coveragesum as numeric) / cast(t5.ecicnt as numeric) as coverprec
         from (
                  select t2.areaid,
                         t2.avgsum + t3.distsum as coveragesum
                  from (
                           select areaid,
                                  sum(case when avgdist > 0.8 then 1 else 0 end) as avgsum
                           from (
                                    select areaid,
                                           avg(cast(top_dist as numeric)) as avgdist
                                    from cnio.siteinfo_area_cell
                                    group by areaid
                                ) t1
                           group by areaid) t2
                           left join
                       (
                           select areaid,
                                  sum(case
                                          when (cast(top_dist as numeric) > 0.5 and cast(rankid as int) = 1)
                                              then 1
                                          else 0 end) distsum
                           from cnio.siteinfo_area_cell
                           group by areaid) t3
                       on t2.areaid = t3.areaid) t4
                  left join(
             select areaid, count(eci) as ecicnt from cnio.siteinfo_area_cell group by areaid
         ) t5
                           on t4.areaid = t5.areaid
     ) m1
         left join
     (
         select sumaz,
                ecisum,
                cast(sumaz as numeric) / cast(ecisum as numeric) as prec,
                areaid
         from (
                  select sumaz, ecisum, t5.areaid
                  from (
                           select sum(case when t2.azimuthcheck != 0 then 1 else 0 end) as sumaz, areaid
                           from (
                                    select distinct eci,
                                                    areaid
                                    from (
                                             select cast(areaid as bigint), cast(eci as integer)
                                             from cnio.siteinfo_area_cell
                                             union all
                                             select  cast(area_id as bigint), cast(eci as integer)
                                             from cnio.poorcoverage_area_cell) tt1
                                ) t1
                                    left join
                                cnio.task_43_lte_cell_sum t2
                                on cast(t1.eci as integer) = cast(t2.eci as integer)
                           group by areaid
                       ) t5
                           left join
                       (
                           select count(distinct eci) as ecisum,
                                  areaid
                           from (
                                    select cast(areaid as bigint), cast(eci as integer)
                                    from cnio.siteinfo_area_cell
                                    union all
                                    select cast(area_id as bigint), cast(eci as integer)
                                    from cnio.poorcoverage_area_cell) t1
                           group by cast(areaid as bigint)
                       ) t4
                       on t5.areaid = t4.areaid
              ) t6
     ) m2
     on cast(m1.areaid as bigint) = cast(m2.areaid as bigint)
         left join
     (
         select locationerr,
                ecisum,
                cast(locationerr as numeric) / cast(ecisum as numeric) as prec,
                areaid
         from (
                  select locationerr, ecisum, t5.areaid
                  from (
                           select sum(case when t2.locationerr != 0 then 1 else 0 end) as locationerr, areaid
                           from (
                                    select distinct eci,
                                                    areaid
                                    from (
                                             select cast(areaid as bigint), cast(eci as integer)
                                             from cnio.siteinfo_area_cell
                                             union all
                                             select  cast(area_id as bigint), cast(eci as integer)
                                             from cnio.poorcoverage_area_cell) tt1
                                ) t1
                                    left join
                                cnio.task_43_lte_cell_sum t2
                                on cast(t1.eci as integer) = cast(t2.eci as integer)
                           group by areaid
                       ) t5
                           left join
                       (
                           select count(distinct eci) as ecisum,
                                  areaid
                           from (
                                    select cast(areaid as bigint), cast(eci as integer)
                                    from cnio.siteinfo_area_cell
                                    union all
                                    select  cast(area_id as bigint), cast(eci as integer)
                                    from cnio.poorcoverage_area_cell) t1
                           group by cast(areaid as bigint)
                       ) t4
                       on t5.areaid = t4.areaid
              ) t6
         ) m3
     on
         cast(m1.areaid as bigint) = cast(m3.areaid as bigint)
         left join
     (
         select overshooting,
                ecisum,
                cast(overshooting as numeric) / cast(ecisum as numeric) as prec,
                areaid
         from (
                  select overshooting, ecisum, t5.areaid
                  from (
                           select sum(case when t2.overshooting != 0 then 1 else 0 end) as overshooting, areaid
                           from (
                                    select distinct eci,
                                                    areaid
                                    from (
                                             select cast(areaid as bigint), cast(eci as integer)
                                             from cnio.siteinfo_area_cell
                                             union all
                                             select  cast(area_id as bigint), cast(eci as integer)
                                             from cnio.poorcoverage_area_cell) tt1
                                ) t1
                                    left join
                                cnio.task_43_lte_cell_sum t2
                                on cast(t1.eci as integer) = cast(t2.eci as integer)
                           group by areaid
                       ) t5
                           left join
                       (
                           select count(distinct eci) as ecisum,
                                  areaid
                           from (
                                    select cast(areaid as bigint), cast(eci as integer)
                                    from cnio.siteinfo_area_cell
                                    union all
                                    select  cast(area_id as bigint), cast(eci as integer)
                                    from cnio.poorcoverage_area_cell) t1
                           group by cast(areaid as bigint)
                       ) t4
                       on t5.areaid = t4.areaid
              ) t6
         ) m4
     on
         cast(m1.areaid as bigint) = cast(m4.areaid as bigint)
         left join
     (
         select area_id,
                sdist_cell_num,
                sdist_cell_sum,
                cast(sdist_cell_num as numeric) / cast(sdist_cell_sum as numeric) as sdist_cell_ratio
         from (
                  select count(eci) as sdist_cell_sum, area_id, sum(distless100) sdist_cell_num
                  from (
                           select distinct eci, area_id, case when cast(dist as double precision) < 100 then 1 else 0 end as distless100
                           from cnio.poorcoverage_area_cell) t1
                  group by area_id) t2
         ) m5
     on
         cast(m1.areaid as bigint) = cast(m5.area_id as bigint)
         left join
     (
         select distinct t8.areaid,
                         t8.eciallcnt,
                         t8.alarmectcnt,
                         CAST(t8.alarmectcnt as numeric) / CAST(t8.eciallcnt as numeric) as prec
         from (
                  select t6.areaid, t4.eciallcnt, sum(alarmectcnt) over(partition by t6.areaid) as alarmectcnt
                  from (

select tt2.eci,count(tt2.eci) as alarmectcnt,tt1.areaid from
(
 select distinct t7.eci,t5.areaid as areaid
                           from (
                                    select t1.areaid, t1.eci
                                    from (
                                             select distinct eci,
                                                             areaid
                                             from (
                                                      select cast(areaid as bigint), cast(eci as integer)
                                                      from cnio.siteinfo_area_cell
                                                      union all
                                                      select cast(area_id as bigint), cast(eci as integer)
                                                      from cnio.poorcoverage_area_cell) tt1
                                         ) t1
                                ) t5

                                    left join cnio.alarm_hz_day t7
                                              on cast(t5.eci as integer) = cast(t7.eci as integer)
                           where t7.eci is not null and  (important_alarm_num is not null and important_alarm_num !=0) or (urgent_alarm_num is not null and urgent_alarm_num !=0) 
                           and data_time = cast(to_char(now()::timestamp  + '-2 day +8 hour', 'yyyy-mm-dd') as date)
                           ) tt1
                            left join cnio.alarm_hz_day tt2 on tt1.eci = tt2.eci group by tt2.eci,tt1.areaid
having count(tt2.eci)>=3
                       ) t6
                           right join
                       (
                           select count(distinct eci) as eciallcnt,
                                  areaid
                           from (
                                    select cast(areaid as bigint), cast(eci as integer)
                                    from cnio.siteinfo_area_cell
                                    union all
                                    select cast(area_id as bigint), cast(eci as integer)
                                    from cnio.poorcoverage_area_cell) t1
                           group by cast(areaid as bigint)
                       ) t4
                       on t6.areaid = t4.areaid
              ) t8) m6
     on cast(m1.areaid as bigint) = cast(m6.areaid as bigint)
         left join
     (
         select area_id, lon_area, lat_area, cnt from cnio.poorcoverage_area_cell group by area_id, lon_area, lat_area, cnt
     ) m7
     on cast(m1.areaid as bigint) = cast(m7.area_id as bigint);

                           
                          
     
insert into  cnio.poorcoverage_area_judgement
select areaid,
       lat,
       lon,
       cnt,
       alarm_cell_num,
       alarm_cell_sum,
       alarm_ratio,
       ldist_cell_num,
       ldist_cell_sum,
       ldist_cell_ratio,
       azimuthcheck_cell_num,
       azimuthcheck_cell_sum,
       azimuthcheck_cell_ratio,
       locationerr_cell_num,
       locationerr_cell_sum,
       locationerr_cell_ratio,
       overshooting_cell_num,
       overshooting_cell_sum,
       overshooting_cell_ratio,
       sdist_cell_num,
       sdist_cell_sum,
       sdist_cell_ratio,
       case
           when cast(alarm_cell_num as text) is not null then '维护'
           else
               (case
                    when cast(alarm_cell_num as text) is null and cast(ldist_cell_ratio as numeric) > 0.1 then '建设'
                    else '优化' end) end as order_sorting
        ,

       case
           when cast(alarm_cell_num as text) is not null then '1. 【小区/基站告警】 6. 【天馈问题】'
           else (case
                     when cast(alarm_cell_num as text) is null and cast(ldist_cell_ratio as numeric) > 0.1
                         then '2. 【网络结构不合理】 6. 【天馈问题】' when cast(alarm_cell_num as text) is not null and cast(ldist_cell_ratio as numeric) > 0.1
                         then '1. 【小区/基站告警】 2. 【网络结构不合理】 6. 【天馈问题】'
                     else (
                         case
                             when cast(alarm_cell_num as text) is null and cast(ldist_cell_ratio as numeric) < 0.1 and
                                  cast(sdist_cell_ratio as numeric) > 0 then '3. 【疑似建筑物阻挡】 6. 【天馈问题】 6. 【天馈问题】'
                             when cast(alarm_cell_num as text) is not null and cast(ldist_cell_ratio as numeric) > 0.1 and
                                  cast(sdist_cell_ratio as numeric) > 0 then '1. 【小区/基站告警】 2. 【网络结构不合理】 3. 【疑似建筑物阻挡】'
                             when cast(alarm_cell_num as text) is not null and cast(ldist_cell_ratio as numeric) < 0.1 and
                                  cast(sdist_cell_ratio as numeric) > 0 then '1. 【小区/基站告警】  3. 【疑似建筑物阻挡】 6. 【天馈问题】'
                             when cast(alarm_cell_num as text) is  null and cast(ldist_cell_ratio as numeric) > 0.1 and
                                  cast(sdist_cell_ratio as numeric) > 0 then '2. 【网络结构不合理】  3. 【疑似建筑物阻挡】 6. 【天馈问题】'
                             else (
                                 case
                                     when cast(alarm_cell_num as text) is null and
                                          cast(ldist_cell_ratio as numeric) < 0.1 and
                                          cast(sdist_cell_ratio as numeric) < 0 and
                                          cast(overshooting_cell_ratio as numeric) > 0 then '4. 【越区覆盖】 6. 【天馈问题】 6. 【天馈问题】'
                                     when cast(alarm_cell_num as text) is not null and cast(ldist_cell_ratio as numeric) > 0.1 and
                                          cast(overshooting_cell_ratio as numeric) < 0 then '1. 【小区/基站告警】 2. 【网络结构不合理】 4. 【越区覆盖】'
                                     when cast(alarm_cell_num as text) is not null and cast(ldist_cell_ratio as numeric) < 0.1 and
                                          cast(sdist_cell_ratio as numeric) > 0 and cast(overshooting_cell_ratio as numeric) > 0 then '1. 【小区/基站告警】 3. 【疑似建筑物阻挡】 4. 【越区覆盖】'
                                     when cast(alarm_cell_num as text) is null and cast(ldist_cell_ratio as numeric) > 0.1 and
                                          cast(sdist_cell_ratio as numeric) > 0 and cast(overshooting_cell_ratio as numeric) > 0 then '2. 【网络结构不合理】  3. 【疑似建筑物阻挡】 4. 【越区覆盖】'
                                     else (
                                         case
                                             when cast(alarm_cell_num as text) is null and
                                                  cast(ldist_cell_ratio as numeric) < 0.1 and
                                                  cast(sdist_cell_ratio as numeric) < 0 and
                                                  cast(overshooting_cell_ratio as numeric) < 0 and
                                                  cast(locationerr_cell_ratio as numeric) > 0 then '5. 【基站位置错误】 6. 【天馈问题】 6. 【天馈问题】'
                                             when cast(alarm_cell_num as text) is not null and cast(ldist_cell_ratio as numeric) > 0.1 and
                                          cast(sdist_cell_ratio as numeric) > 0 and cast(overshooting_cell_ratio as numeric) < 0 and cast(locationerr_cell_ratio as numeric) < 0 then '1. 【小区/基站告警】 2. 【网络结构不合理】 3.【疑似建筑物阻挡】'
                                             when cast(alarm_cell_num as text) is not null and cast(ldist_cell_ratio as numeric) > 0.1 and
                                          cast(sdist_cell_ratio as numeric) < 0 and cast(overshooting_cell_ratio as numeric) > 0 and cast(locationerr_cell_ratio as numeric) < 0 then '1. 【小区/基站告警】 2. 【网络结构不合理】 4. 【越区覆盖】'
                                             when cast(alarm_cell_num as text) is not null and cast(ldist_cell_ratio as numeric) > 0.1 and
                                          cast(sdist_cell_ratio as numeric) < 0 and cast(overshooting_cell_ratio as numeric) < 0 and cast(locationerr_cell_ratio as numeric) < 0 then '1. 【小区/基站告警】 2. 【网络结构不合理】 5. 【基站位置错误】'
                                             when cast(alarm_cell_num as text) is not null and cast(ldist_cell_ratio as numeric) < 0.1 and
                                          cast(sdist_cell_ratio as numeric) > 0 and cast(overshooting_cell_ratio as numeric) > 0 and cast(locationerr_cell_ratio as numeric) < 0 then '1. 【小区/基站告警】 3. 【疑似建筑物阻挡】 4. 【越区覆盖】 '
                                             when cast(alarm_cell_num as text) is not null and cast(ldist_cell_ratio as numeric) < 0.1 and
                                          cast(sdist_cell_ratio as numeric) > 0 and cast(overshooting_cell_ratio as numeric) < 0 and cast(locationerr_cell_ratio as numeric) > 0 then '1. 【小区/基站告警】 3. 【疑似建筑物阻挡】 5. 【基站位置错误】'
                                             when cast(alarm_cell_num as text) is  null and cast(ldist_cell_ratio as numeric) > 0.1 and
                                          cast(sdist_cell_ratio as numeric) > 0 and cast(overshooting_cell_ratio as numeric) < 0 and cast(locationerr_cell_ratio as numeric) < 0 then '2. 【网络结构不合理】 3. 【疑似建筑物阻挡】 4. 【越区覆盖】'
                                             when cast(alarm_cell_num as text) is  null and cast(ldist_cell_ratio as numeric) > 0.1 and
                                          cast(sdist_cell_ratio as numeric) > 0 and cast(overshooting_cell_ratio as numeric) < 0 and cast(locationerr_cell_ratio as numeric) > 0 then '2. 【网络结构不合理】 3. 【疑似建筑物阻挡】 5. 【基站位置错误】'
                                             when cast(alarm_cell_num as text) is  null and cast(ldist_cell_ratio as numeric) > 0.1 and
                                          cast(sdist_cell_ratio as numeric) < 0 and cast(overshooting_cell_ratio as numeric) > 0 and cast(locationerr_cell_ratio as numeric) > 0 then '2. 【网络结构不合理】 4. 【越区覆盖】 5. 【基站位置错误】'
                                             when cast(alarm_cell_num as text) is  null and cast(ldist_cell_ratio as numeric) < 0.1 and
                                          cast(sdist_cell_ratio as numeric) > 0 and cast(overshooting_cell_ratio as numeric) > 0 and cast(locationerr_cell_ratio as numeric) > 0 then '3. 【疑似建筑物阻挡】 4. 【越区覆盖】 5. 【基站位置错误】'

                                             else '6. 【天馈问题】' end
                                         ) end
                                 ) end
                         ) end) end    as root_cause,

       case
           when cast(alarm_cell_num as text) is not null then '1. 恢复小区影响业务故障 6. 异常的天线方位角/波瓣角调整 '
           else (case
                     when cast(alarm_cell_num as text) is null and cast(ldist_cell_ratio as numeric) > 0.1
                         then '2. 新建基站，补充覆盖 6. 异常的天线方位角/波瓣角调整' when cast(alarm_cell_num as text) is not null and cast(ldist_cell_ratio as numeric) > 0.1
                         then '1. 恢复小区影响业务故障 2. 新建基站，补充覆盖 6. 异常的天线方位角/波瓣角调整'
                     else (
                         case
                             when cast(alarm_cell_num as text) is null and cast(ldist_cell_ratio as numeric) < 0.1 and
                                  cast(sdist_cell_ratio as numeric) > 0 then '3. 新建基站/RRU拉远补充/天馈调整 6. 异常的天线方位角/波瓣角调整 6. 异常的天线方位角/波瓣角调整'
                             when cast(alarm_cell_num as text) is not null and cast(ldist_cell_ratio as numeric) > 0.1 and
                                  cast(sdist_cell_ratio as numeric) > 0 then '1. 恢复小区影响业务故障 2. 新建基站，补充覆盖 3. 新建基站/RRU拉远补充/天馈调整'
                             when cast(alarm_cell_num as text) is not null and cast(ldist_cell_ratio as numeric) < 0.1 and
                                  cast(sdist_cell_ratio as numeric) > 0 then '1. 恢复小区影响业务故障  3. 新建基站/RRU拉远补充/天馈调整 6. 异常的天线方位角/波瓣角调整'
                             when cast(alarm_cell_num as text) is  null and cast(ldist_cell_ratio as numeric) > 0.1 and
                                  cast(sdist_cell_ratio as numeric) > 0 then '2. 新建基站，补充覆盖  3. 新建基站/RRU拉远补充/天馈调整 6. 异常的天线方位角/波瓣角调整'
                             else (
                                 case
                                     when cast(alarm_cell_num as text) is null and
                                          cast(ldist_cell_ratio as numeric) < 0.1 and
                                          cast(sdist_cell_ratio as numeric) < 0 and
                                          cast(overshooting_cell_ratio as numeric) > 0 then '4. 天馈调整 6. 异常的天线方位角/波瓣角调整 6. 异常的天线方位角/波瓣角调整'
                                     when cast(alarm_cell_num as text) is not null and cast(ldist_cell_ratio as numeric) > 0.1 and
                                          cast(overshooting_cell_ratio as numeric) < 0 then '1. 恢复小区影响业务故障 2. 新建基站，补充覆盖 4. 天馈调整'
                                     when cast(alarm_cell_num as text) is not null and cast(ldist_cell_ratio as numeric) < 0.1 and
                                          cast(sdist_cell_ratio as numeric) > 0 and cast(overshooting_cell_ratio as numeric) > 0 then '1. 恢复小区影响业务故障 3. 新建基站/RRU拉远补充/天馈调整 4. 天馈调整'
                                     when cast(alarm_cell_num as text) is null and cast(ldist_cell_ratio as numeric) > 0.1 and
                                          cast(sdist_cell_ratio as numeric) > 0 and cast(overshooting_cell_ratio as numeric) > 0 then '2. 新建基站，补充覆盖  3. 新建基站/RRU拉远补充/天馈调整 4. 天馈调整'
                                     else (
                                         case
                                             when cast(alarm_cell_num as text) is null and
                                                  cast(ldist_cell_ratio as numeric) < 0.1 and
                                                  cast(sdist_cell_ratio as numeric) < 0 and
                                                  cast(overshooting_cell_ratio as numeric) < 0 and
                                                  cast(locationerr_cell_ratio as numeric) > 0 then '5. 核实修改工参数据 6. 异常的天线方位角/波瓣角调整 6. 异常的天线方位角/波瓣角调整'
                                             when cast(alarm_cell_num as text) is not null and cast(ldist_cell_ratio as numeric) > 0.1 and
                                          cast(sdist_cell_ratio as numeric) > 0 and cast(overshooting_cell_ratio as numeric) < 0 and cast(locationerr_cell_ratio as numeric) < 0 then '1. 恢复小区影响业务故障 2. 新建基站，补充覆盖 3.新建基站/RRU拉远补充/天馈调整'
                                             when cast(alarm_cell_num as text) is not null and cast(ldist_cell_ratio as numeric) > 0.1 and
                                          cast(sdist_cell_ratio as numeric) < 0 and cast(overshooting_cell_ratio as numeric) > 0 and cast(locationerr_cell_ratio as numeric) < 0 then '1. 恢复小区影响业务故障 2. 新建基站，补充覆盖 4. 天馈调整'
                                             when cast(alarm_cell_num as text) is not null and cast(ldist_cell_ratio as numeric) > 0.1 and
                                          cast(sdist_cell_ratio as numeric) < 0 and cast(overshooting_cell_ratio as numeric) < 0 and cast(locationerr_cell_ratio as numeric) < 0 then '1. 恢复小区影响业务故障 2. 新建基站，补充覆盖 5. 核实修改工参数据'
                                             when cast(alarm_cell_num as text) is not null and cast(ldist_cell_ratio as numeric) < 0.1 and
                                          cast(sdist_cell_ratio as numeric) > 0 and cast(overshooting_cell_ratio as numeric) > 0 and cast(locationerr_cell_ratio as numeric) < 0 then '1. 恢复小区影响业务故障 3. 新建基站/RRU拉远补充/天馈调整 4. 天馈调整 '
                                             when cast(alarm_cell_num as text) is not null and cast(ldist_cell_ratio as numeric) < 0.1 and
                                          cast(sdist_cell_ratio as numeric) > 0 and cast(overshooting_cell_ratio as numeric) < 0 and cast(locationerr_cell_ratio as numeric) > 0 then '1. 恢复小区影响业务故障 3. 新建基站/RRU拉远补充/天馈调整 5. 核实修改工参数据'
                                             when cast(alarm_cell_num as text) is  null and cast(ldist_cell_ratio as numeric) > 0.1 and
                                          cast(sdist_cell_ratio as numeric) > 0 and cast(overshooting_cell_ratio as numeric) < 0 and cast(locationerr_cell_ratio as numeric) < 0 then '2. 新建基站，补充覆盖 3. 新建基站/RRU拉远补充/天馈调整 4. 天馈调整'
                                             when cast(alarm_cell_num as text) is  null and cast(ldist_cell_ratio as numeric) > 0.1 and
                                          cast(sdist_cell_ratio as numeric) > 0 and cast(overshooting_cell_ratio as numeric) < 0 and cast(locationerr_cell_ratio as numeric) > 0 then '2. 新建基站，补充覆盖 3. 新建基站/RRU拉远补充/天馈调整 5. 核实修改工参数据'
                                             when cast(alarm_cell_num as text) is  null and cast(ldist_cell_ratio as numeric) > 0.1 and
                                          cast(sdist_cell_ratio as numeric) < 0 and cast(overshooting_cell_ratio as numeric) > 0 and cast(locationerr_cell_ratio as numeric) > 0 then '2. 新建基站，补充覆盖 4. 天馈调整 5. 核实修改工参数据'
                                             when cast(alarm_cell_num as text) is  null and cast(ldist_cell_ratio as numeric) < 0.1 and
                                          cast(sdist_cell_ratio as numeric) > 0 and cast(overshooting_cell_ratio as numeric) > 0 and cast(locationerr_cell_ratio as numeric) > 0 then '3. 新建基站/RRU拉远补充/天馈调整 4. 天馈调整 5. 核实修改工参数据'

                                             else '6. 异常的天线方位角/波瓣角调整' end
                                         ) end
                                 ) end
                         ) end) end    as tuning_action,

        case
           when cast(alarm_cell_num as text) is not null then '区域范围内服务小区/基站出现告警'
           else (
               case
                   when cast(alarm_cell_num as text) is null and cast(ldist_cell_ratio as numeric) > 0.1
                       then '站间距过大，需要新建站'
                   else (
                       case
                           when cast(alarm_cell_num as text) is null and cast(ldist_cell_ratio as numeric) < 0.1 and
                                cast(sdist_cell_ratio as numeric) > 0
                               then '近距离弱覆盖，疑似建筑物阻挡'
                           else (
                               case
                                   when cast(alarm_cell_num as text) is null and
                                        cast(ldist_cell_ratio as numeric) < 0.1 and
                                        cast(sdist_cell_ratio as numeric) < 0 and
                                        cast(overshooting_cell_ratio as numeric) > 0 then '越区覆盖，造成弱覆盖'
                                   else (
                                       case
                                           when cast(alarm_cell_num as text) is null and
                                                cast(ldist_cell_ratio as numeric) < 0.1 and
                                                cast(sdist_cell_ratio as numeric) < 0 and
                                                cast(overshooting_cell_ratio as numeric) < 0
                                               then '基站位置错误，核实工参数据'
                                           else '核查天馈问题' end
                                       ) end
                               ) end
                       ) end
               ) end                   as comments,

       cast(to_char(now()::timestamp  + '-2 day +8 hour', 'yyyy-mm-dd') as date)            as order_date
from cnio.poorcoverage_cell_weakcoverageall;

