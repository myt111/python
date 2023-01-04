import trino
from trino import transaction
import pandas as pd
import psycopg2
import datetime as dt
from io import StringIO
from sqlalchemy import create_engine

def get_date_list(days_num):
    now_time = dt.datetime.now()
    date_list = []
    count=0
    while count < (-days_num):
        now_time = now_time + dt.timedelta(days=-1)
        temp_date = now_time.strftime('%Y%m%d')
        if count==0:
            max_date=temp_date
        date_list.append(temp_date)
        min_date = temp_date
        count=count+1
    print("get date list {0}".format(date_list))
    date_part = (int(min_date), int(max_date))
    return date_list, date_part

def get_upload_date_list(date_list):
    # 寤虹珛杩炴帴
    conn = psycopg2.connect(host="133.160.191.111", user="expert-system", password="YJY_exp#exp502",
                            database="expert-system", port="5432")
    cur = conn.cursor()
    sql = """
        select
        distinct cast(daytime  as varchar) as daytime
    from
        searching.pm_4g_zdy_day pgzh 
    where
        daytime  between %s and %s
    """ % date_list
    cur.execute(sql)
    data = cur.fetchall()

    if len(data) != 0:
        data = pd.DataFrame(data)
        data.columns = ["daytime"]
        data = data["daytime"].tolist()
        print("已上传数据时间读取完毕")

    else:
        print("已上传数据时间读取失败")
        data = []

    print("upload day {0}".format(data))
    cur.close()
    conn.close()
    return data

def get_not_upload_date_list(date_list, upload_date_list):
    not_uploaded_date_list = []
    for i in range(len(date_list)):
        print("date_list {0}".format(date_list[i]))
        if date_list[i] in upload_date_list:
            continue
        else:
            not_uploaded_date_list.append(date_list[i])
    return not_uploaded_date_list

## 判断pm数据是否有效
def pm_data_is_effective(date):
    date = int(date)
    conn = psycopg2.connect(host="133.160.191.111", user="expert-system", password="YJY_exp#exp502",
                            database="expert-system", port="5432")
    cur = conn.cursor()

    ## 工参获取
    sql = """
        select
        count(distinct hourtime) as hour_num
    from
        searching.pm_4g_zdy_hour pgzh
    where
        daytime = %s
    """ % date
    cur.execute(sql)
    data = cur.fetchall()
    if len(data):
        data = pd.DataFrame(data)
        data.columns = ["hour_num"]
        print("{0}日pm数据有效小时获取成功".format(date))
        if data.loc[0,"hour_num"]>=20:
            flag=1
        else:
            flag=0
    else:
        print("{0}日pm数据有效小时获取失败".format(date))
        flag=0
    cur.close()
    conn.close()
    return flag

def sync_pm_data(sql):
    conn = psycopg2.connect(host="133.160.191.111", user="expert-system", password="YJY_exp#exp502",
                            database="expert-system", port="5432")
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()


def data_process(not_upload_date_list):
    for i in range(len(not_upload_date_list)):
        sql = ''' 
        insert into "expert-system".searching.pm_4g_zdy_day
        select 
            province_id,
            city_id,
            city_name,
            daytime,
            oid,
            related_gnb,
            cellid,
            vendor_id,
            network_type,
            sum(kpi_0000000001) as kpi_0000000001,
            sum(kpi_0000000008) as kpi_0000000008,
            sum(kpi_0000000015) as kpi_0000000015,
            sum(kpi_0000000021) as kpi_0000000021,
            sum(kpi_0000000027) as kpi_0000000027,
            sum(kpi_0000000028) as kpi_0000000028,
            sum(kpi_0000000029) as kpi_0000000029,
            sum(kpi_0000000030) as kpi_0000000030,
            sum(kpi_0000000031) as kpi_0000000031,
            sum(kpi_0000000032) as kpi_0000000032,
            sum(kpi_0000000033) as kpi_0000000033,
            sum(kpi_0000000034) as kpi_0000000034,
            sum(kpi_0000000035) as kpi_0000000035,
            sum(kpi_0000000036) as kpi_0000000036,
            sum(kpi_0000000037) as kpi_0000000037,
            sum(kpi_0000000038) as kpi_0000000038,
            sum(kpi_0000000039) as kpi_0000000039,
            sum(kpi_0000000040) as kpi_0000000040,
            sum(kpi_0000000041) as kpi_0000000041,
            sum(kpi_0000000042) as kpi_0000000042,
            sum(kpi_0000000043) as kpi_0000000043,
            sum(kpi_0000000044) as kpi_0000000044,
            sum(kpi_0000000045) as kpi_0000000045,
            sum(kpi_0000000046) as kpi_0000000046,
            sum(kpi_0000000047) as kpi_0000000047,
            sum(kpi_0000000047_pucch) as kpi_0000000047_pucch,
            sum(kpi_0000000047_srs) as kpi_0000000047_srs,
            sum(kpi_0000000047_userspace) as kpi_0000000047_userspace,
            sum(kpi_0000000048) as kpi_0000000048,
            sum(kpi_0000000049) as kpi_0000000049,
            sum(kpi_0000000050) as kpi_0000000050,
            sum(kpi_0000000051) as kpi_0000000051,
            sum(kpi_0000000055) as kpi_0000000055,
            sum(kpi_0000000060) as kpi_0000000060,
            sum(kpi_0000000061) as kpi_0000000061,
            sum(kpi_0000000065) as kpi_0000000065,
            sum(kpi_0000000071) as kpi_0000000071,
            sum(kpi_0000000075) as kpi_0000000075,
            sum(kpi_0000000081) as kpi_0000000081,
            sum(kpi_0000000085) as kpi_0000000085,
            avg(kpi_0000000090) as kpi_0000000090,
            sum(kpi_0000000121) as kpi_0000000121,
            sum(kpi_0000000122) as kpi_0000000122,
            sum(kpi_0000000123) as kpi_0000000123,
            sum(kpi_0000000133) as kpi_0000000133,
            sum(kpi_0000000134) as kpi_0000000134,
            sum(kpi_0000000138) as kpi_0000000138,
            sum(kpi_0000000143) as kpi_0000000143,
            sum(kpi_0000000144) as kpi_0000000144,
            sum(kpi_0000000148) as kpi_0000000148,
            sum(kpi_0000000153) as kpi_0000000153,
            sum(kpi_0000000153_v) as kpi_0000000153_v,
            sum(kpi_0000000154) as kpi_0000000154,
            sum(kpi_0000000155) as kpi_0000000155,
            sum(kpi_0000000156) as kpi_0000000156,
            sum(kpi_0000000157) as kpi_0000000157,
            sum(kpi_0000000158) as kpi_0000000158,
            sum(kpi_0000000158_v) as kpi_0000000158_v,
            sum(kpi_0000000159) as kpi_0000000159,
            sum(kpi_0000000160) as kpi_0000000160,
            sum(kpi_0000000161) as kpi_0000000161,
            sum(kpi_0000000162) as kpi_0000000162,
            sum(kpi_0000000163) as kpi_0000000163,
            sum(kpi_0000000164) as kpi_0000000164,
            sum(kpi_0000000165) as kpi_0000000165,
            sum(kpi_0000000166) as kpi_0000000166,
            sum(kpi_0000000167) as kpi_0000000167,
            sum(kpi_0000000168) as kpi_0000000168,
            sum(kpi_0000000169) as kpi_0000000169,
            sum(kpi_0000000170) as kpi_0000000170,
            sum(kpi_0000000171) as kpi_0000000171,
            sum(kpi_0000000172) as kpi_0000000172,
            sum(kpi_0000000176) as kpi_0000000176,
            sum(kpi_0000000181) as kpi_0000000181,
            sum(kpi_0000000181_hofailure) as kpi_0000000181_hofailure,
            sum(kpi_0000000182) as kpi_0000000182,
            sum(kpi_0000000186) as kpi_0000000186,
            sum(kpi_0000000191) as kpi_0000000191,
            sum(kpi_0000000192) as kpi_0000000192,
            sum(kpi_0000000193) as kpi_0000000193,
            sum(kpi_0000000194) as kpi_0000000194,
            sum(kpi_0000000195) as kpi_0000000195,
            sum(kpi_0000000196) as kpi_0000000196,
            sum(kpi_0000000197) as kpi_0000000197,
            sum(kpi_0000000198) as kpi_0000000198,
            sum(kpi_0000000199) as kpi_0000000199,
            sum(kpi_0000000200) as kpi_0000000200,
            sum(kpi_0000000201) as kpi_0000000201,
            sum(kpi_0000000205) as kpi_0000000205,
            avg(kpi_0000000210) as kpi_0000000210,
            sum(kpi_0000000220) as kpi_0000000220,
            sum(kpi_0000000220_rnl) as kpi_0000000220_rnl,
            sum(kpi_0000000221) as kpi_0000000221,
            sum(kpi_0000000222) as kpi_0000000222,
            sum(kpi_0000000223) as kpi_0000000223,
            sum(kpi_0000000224) as kpi_0000000224,
            sum(kpi_0000000225) as kpi_0000000225,
            sum(kpi_0000000226) as kpi_0000000226,
            sum(kpi_0000000227) as kpi_0000000227,
            sum(kpi_0000000228) as kpi_0000000228,
            sum(kpi_0000000228_v) as kpi_0000000228_v,
            sum(kpi_0000000229) as kpi_0000000229,
            sum(kpi_0000000229_v) as kpi_0000000229_v,
            sum(kpi_0000000230) as kpi_0000000230,
            sum(kpi_0000000230_v) as kpi_0000000230_v,
            sum(kpi_0000000231) as kpi_0000000231,
            sum(kpi_0000000231_v) as kpi_0000000231_v,
            sum(kpi_0000000232) as kpi_0000000232,
            sum(kpi_0000000233) as kpi_0000000233,
            sum(kpi_0000000237) as kpi_0000000237,
            sum(kpi_0000000242) as kpi_0000000242,
            sum(kpi_0000000243) as kpi_0000000243,
            sum(kpi_0000000247) as kpi_0000000247,
            sum(kpi_0000000252) as kpi_0000000252,
            sum(kpi_0000000253) as kpi_0000000253,
            sum(kpi_0000000257) as kpi_0000000257,
            sum(kpi_0000000262) as kpi_0000000262,
            sum(kpi_0000000263) as kpi_0000000263,
            sum(kpi_0000000267) as kpi_0000000267,
            sum(kpi_0000000272) as kpi_0000000272,
            sum(kpi_0000000273) as kpi_0000000273,
            sum(kpi_0000000274) as kpi_0000000274,
            sum(kpi_0000000275) as kpi_0000000275,
            sum(kpi_0000000276) as kpi_0000000276,
            sum(kpi_0000000277) as kpi_0000000277,
            sum(kpi_0000000278) as kpi_0000000278,
            sum(kpi_0000000279) as kpi_0000000279,
            sum(kpi_0000000280) as kpi_0000000280,
            sum(kpi_0000000281) as kpi_0000000281,
            sum(kpi_0000000282) as kpi_0000000282,
            sum(kpi_0000000283) as kpi_0000000283,
            sum(kpi_0000000284) as kpi_0000000284,
            sum(kpi_0000000285) as kpi_0000000285,
            sum(kpi_0000000286) as kpi_0000000286,
            sum(kpi_0000000287) as kpi_0000000287,
            sum(kpi_0000000288) as kpi_0000000288,
            sum(kpi_0000000289) as kpi_0000000289,
            sum(kpi_0000000290) as kpi_0000000290,
            sum(kpi_0000000291) as kpi_0000000291,
            sum(kpi_0000000292) as kpi_0000000292,
            sum(kpi_0000000293) as kpi_0000000293,
            sum(kpi_0000000294) as kpi_0000000294,
            sum(kpi_0000000295) as kpi_0000000295,
            sum(kpi_0000000296) as kpi_0000000296,
            sum(kpi_0000000297) as kpi_0000000297,
            sum(kpi_0000000298) as kpi_0000000298,
            sum(kpi_0000000299) as kpi_0000000299,
            sum(kpi_0000000300) as kpi_0000000300,
            sum(kpi_0000000301) as kpi_0000000301,
            sum(kpi_0000000302) as kpi_0000000302,
            sum(kpi_0000000303) as kpi_0000000303,
            sum(kpi_0000000304) as kpi_0000000304,
            sum(kpi_0000000305) as kpi_0000000305,
            sum(kpi_0000000306) as kpi_0000000306,
            sum(kpi_0000000307) as kpi_0000000307,
            sum(kpi_0000000308) as kpi_0000000308,
            sum(kpi_0000000309) as kpi_0000000309,
            sum(kpi_0000000310) as kpi_0000000310,
            sum(kpi_0000000311) as kpi_0000000311,
            sum(kpi_0000000312) as kpi_0000000312,
            sum(kpi_0000000313) as kpi_0000000313,
            sum(kpi_0000000314) as kpi_0000000314,
            sum(kpi_0000000315) as kpi_0000000315,
            sum(kpi_0000000316) as kpi_0000000316,
            sum(kpi_0000000317) as kpi_0000000317,
            sum(kpi_0000000318) as kpi_0000000318,
            sum(kpi_0000000319) as kpi_0000000319,
            avg(kpi_0000000320) as kpi_0000000320,
            avg(kpi_0000000321) as kpi_0000000321,
            avg(kpi_0000000322) as kpi_0000000322,
            avg(kpi_0000000323) as kpi_0000000323,
            avg(kpi_0000000324) as kpi_0000000324,
            avg(kpi_0000000325) as kpi_0000000325,
            avg(kpi_0000000326) as kpi_0000000326,
            avg(kpi_0000000327) as kpi_0000000327,
            avg(kpi_0000000328) as kpi_0000000328,
            avg(kpi_0000000329) as kpi_0000000329,
            sum(kpi_0000000330) as kpi_0000000330,
            sum(kpi_0000000331) as kpi_0000000331,
            sum(kpi_0000000332) as kpi_0000000332,
            sum(kpi_0000000336) as kpi_0000000336,
            sum(kpi_0000000342) as kpi_0000000342,
            sum(kpi_0000000346) as kpi_0000000346,
            sum(kpi_0000000352) as kpi_0000000352,
            sum(kpi_0000000356) as kpi_0000000356,
            sum(kpi_0000000362) as kpi_0000000362,
            sum(kpi_0000000366) as kpi_0000000366,
            sum(kpi_0000000372) as kpi_0000000372,
            sum(kpi_0000000376) as kpi_0000000376,
            avg(kpi_0000000381) as kpi_0000000381,
            avg(kpi_0000000383) as kpi_0000000383,
            avg(kpi_0000000385) as kpi_0000000385,
            avg(kpi_0000000386) as kpi_0000000386,
            avg(kpi_0000000390) as kpi_0000000390,
            avg(kpi_0000000395) as kpi_0000000395,
            avg(kpi_0000000395_1) as kpi_0000000395_1,
            avg(kpi_0000000395_5) as kpi_0000000395_5,
            avg(kpi_0000000396) as kpi_0000000396,
            avg(kpi_0000000396_1) as kpi_0000000396_1,
            avg(kpi_0000000396_5) as kpi_0000000396_5,
            avg(kpi_0000000397) as kpi_0000000397,
            avg(kpi_0000000401) as kpi_0000000401,
            avg(kpi_0000000406) as kpi_0000000406,
            avg(kpi_0000000406_1) as kpi_0000000406_1,
            avg(kpi_0000000406_5) as kpi_0000000406_5,
            avg(kpi_0000000475) as kpi_0000000475,
            avg(kpi_0000000476) as kpi_0000000476,
            avg(kpi_0000000477) as kpi_0000000477,
            avg(kpi_0000000480) as kpi_0000000480,
            avg(kpi_0000000483) as kpi_0000000483,
            avg(kpi_0000000486) as kpi_0000000486,
            avg(kpi_0000000487) as kpi_0000000487,
            avg(kpi_0000000490) as kpi_0000000490,
            avg(kpi_0000000493) as kpi_0000000493,
            avg(kpi_0000000496) as kpi_0000000496,
            avg(kpi_0000000497) as kpi_0000000497,
            avg(kpi_0000000498) as kpi_0000000498,
            avg(kpi_0000000508) as kpi_0000000508,
            sum(kpi_0000000518) as kpi_0000000518,
            sum(kpi_0000000518_1) as kpi_0000000518_1,
            sum(kpi_0000000518_5) as kpi_0000000518_5,
            sum(kpi_0000000519) as kpi_0000000519,
            sum(kpi_0000000522) as kpi_0000000522,
            sum(kpi_0000000523) as kpi_0000000523,
            sum(kpi_0000000524) as kpi_0000000524,
            sum(kpi_0000000543) as kpi_0000000543,
            sum(kpi_0000000543_1) as kpi_0000000543_1,
            sum(kpi_0000000543_5) as kpi_0000000543_5,
            sum(kpi_0000000544) as kpi_0000000544,
            sum(kpi_0000000545) as kpi_0000000545,
            sum(kpi_0000000546) as kpi_0000000546,
            sum(kpi_0000000547) as kpi_0000000547,
            sum(kpi_0000000547_1) as kpi_0000000547_1,
            sum(kpi_0000000547_5) as kpi_0000000547_5,
            sum(kpi_0000000551) as kpi_0000000551,
            sum(kpi_0000000551_k) as kpi_0000000551_k,
            sum(kpi_0000000552) as kpi_0000000552,
            sum(kpi_0000000553) as kpi_0000000553,
            sum(kpi_0000000554) as kpi_0000000554,
            sum(kpi_0000000555) as kpi_0000000555,
            sum(kpi_0000000556) as kpi_0000000556,
            sum(kpi_0000000557) as kpi_0000000557,
            sum(kpi_0000000558) as kpi_0000000558,
            sum(kpi_0000000559) as kpi_0000000559,
            sum(kpi_0000000560) as kpi_0000000560,
            avg(kpi_0000000577) as kpi_0000000577,
            avg(kpi_0000000578) as kpi_0000000578,
            sum(kpi_0000000579) as kpi_0000000579,
            sum(kpi_0000000582) as kpi_0000000582,
            sum(kpi_0000000583) as kpi_0000000583,
            sum(kpi_0000000584) as kpi_0000000584,
            sum(kpi_0000000585) as kpi_0000000585,
            sum(kpi_0000000585_k) as kpi_0000000585_k,
            sum(kpi_0000000586) as kpi_0000000586,
            sum(kpi_0000000587) as kpi_0000000587,
            sum(kpi_0000000588) as kpi_0000000588,
            sum(kpi_0000000589) as kpi_0000000589,
            sum(kpi_0000000590) as kpi_0000000590,
            sum(kpi_0000000591) as kpi_0000000591,
            sum(kpi_0000000592) as kpi_0000000592,
            avg(kpi_0000000595) as kpi_0000000595,
            avg(kpi_0000000596) as kpi_0000000596,
            avg(kpi_0000000597) as kpi_0000000597,
            avg(kpi_0000000598) as kpi_0000000598,
            avg(kpi_0000000599) as kpi_0000000599,
            avg(kpi_0000000600) as kpi_0000000600,
            avg(kpi_0000000601) as kpi_0000000601,
            avg(kpi_0000000602) as kpi_0000000602,
            avg(kpi_0000000603) as kpi_0000000603,
            avg(kpi_0000000604) as kpi_0000000604,
            avg(kpi_0000000605) as kpi_0000000605,
            avg(kpi_0000000606) as kpi_0000000606,
            sum(kpi_0000000607) as kpi_0000000607,
            sum(kpi_0000000608) as kpi_0000000608,
            sum(kpi_0000000609) as kpi_0000000609,
            sum(kpi_0000000610) as kpi_0000000610,
            sum(kpi_0000000611) as kpi_0000000611,
            sum(kpi_0000000612) as kpi_0000000612,
            sum(kpi_0000000613) as kpi_0000000613,
            sum(kpi_0000000614) as kpi_0000000614,
            sum(kpi_0000000615) as kpi_0000000615,
            sum(kpi_0000000616) as kpi_0000000616,
            sum(kpi_0000000617) as kpi_0000000617,
            sum(kpi_0000000617_cu) as kpi_0000000617_cu,
            sum(kpi_0000000617_du) as kpi_0000000617_du,
            sum(kpi_0000000617_uu) as kpi_0000000617_uu,
            avg(kpi_0000000618) as kpi_0000000618,
            sum(kpi_0000000619) as kpi_0000000619,
            avg(kpi_0000000620) as kpi_0000000620,
            avg(kpi_0000000623) as kpi_0000000623,
            avg(kpi_0000000624) as kpi_0000000624,
            avg(kpi_0000000625) as kpi_0000000625,
            avg(kpi_0000000625_m) as kpi_0000000625_m,
            avg(kpi_0000000625_s) as kpi_0000000625_s,
            sum(kpi_0000000626) as kpi_0000000626,
            sum(kpi_0000000627) as kpi_0000000627,
            avg(kpi_0000000628) as kpi_0000000628,
            sum(kpi_0000000629) as kpi_0000000629,
            sum(kpi_0000000630) as kpi_0000000630,
            sum(kpi_0000000631) as kpi_0000000631,
            avg(kpi_0000000652) as kpi_0000000652,
            sum(kpi_0000000653) as kpi_0000000653,
            avg(kpi_0000000654) as kpi_0000000654,
            avg(kpi_0000000655) as kpi_0000000655,
            sum(kpi_0000000684) as kpi_0000000684,
            sum(kpi_0000000685) as kpi_0000000685,
            sum(kpi_0000000686) as kpi_0000000686,
            sum(kpi_0000000687) as kpi_0000000687,
            avg(pusch_mcsge11_ratio) as pusch_mcsge11_ratio,
            avg(pdsch_mcsge11_ratio) as pdsch_mcsge11_ratio,
            avg(cqig7_ratio) as cqig7_ratio,
            sum(kpi_0000000422) as kpi_0000000422,
            sum(kpi_0000000423) as kpi_0000000423,
            sum(kpi_0000000424) as kpi_0000000424,
            sum(kpi_0000000425) as kpi_0000000425,
            sum(kpi_0000000426) as kpi_0000000426,
            sum(kpi_0000000427) as kpi_0000000427,
            sum(kpi_0000000428) as kpi_0000000428,
            sum(kpi_0000000429) as kpi_0000000429,
            sum(kpi_0000000430) as kpi_0000000430,
            sum(kpi_0000000431) as kpi_0000000431,
            sum(kpi_0000000432) as kpi_0000000432,
            sum(kpi_0000000433) as kpi_0000000433,
            sum(kpi_0000000434) as kpi_0000000434,
            sum(kpi_0000000435) as kpi_0000000435,
            sum(kpi_0000000436) as kpi_0000000436,
            sum(kpi_0000000437) as kpi_0000000437,
            sum(kpi_0000000438) as kpi_0000000438,
            sum(kpi_0000000439) as kpi_0000000439,
            sum(kpi_0000000440) as kpi_0000000440,
            sum(kpi_0000000441) as kpi_0000000441,
            sum(kpi_0000000442) as kpi_0000000442,
            sum(kpi_0000000454) as kpi_0000000454,
            sum(kpi_0000000455) as kpi_0000000455,
            sum(kpi_0000000456) as kpi_0000000456,
            sum(kpi_0000000457) as kpi_0000000457,
            sum(kpi_0000000458) as kpi_0000000458,
            sum(kpi_0000000459) as kpi_0000000459,
            sum(kpi_0000000460) as kpi_0000000460,
            sum(kpi_0000000461) as kpi_0000000461,
            sum(kpi_0000000462) as kpi_0000000462,
            sum(kpi_0000000463) as kpi_0000000463,
            sum(kpi_0000000464) as kpi_0000000464,
            sum(kpi_0000000465) as kpi_0000000465,
            sum(kpi_0000000466) as kpi_0000000466,
            sum(kpi_0000000467) as kpi_0000000467,
            sum(kpi_0000000468) as kpi_0000000468,
            sum(kpi_0000000469) as kpi_0000000469,
            sum(kpi_0000000470) as kpi_0000000470,
            sum(kpi_0000000471) as kpi_0000000471,
            sum(kpi_0000000472) as kpi_0000000472,
            sum(kpi_0000000473) as kpi_0000000473,
            sum(kpi_0000000474) as kpi_0000000474,
            sum(kpi_0000000643) as kpi_0000000643,
            sum(kpi_0000000644) as kpi_0000000644,
            sum(kpi_0000000645) as kpi_0000000645,
            sum(kpi_0000000646) as kpi_0000000646,
            sum(kpi_0000000647) as kpi_0000000647,
            sum(kpi_0000000648) as kpi_0000000648,
            sum(kpi_0000000649) as kpi_0000000649,
            sum(kpi_0000000650) as kpi_0000000650,
            sum(kpi_0000000651) as kpi_0000000651,
            cell_name
        from "expert-system".searching.pm_4g_zdy_hour
        where daytime = cast(%s as int)
        group by 
            province_id,
            city_id,
            city_name,
            daytime,
            oid,
            related_gnb,
            cellid,
            vendor_id,
            network_type,
            cell_name

         ''' % not_upload_date_list[i]
        effective_flag=pm_data_is_effective(not_upload_date_list[i])
        if effective_flag==1:
            try:
                sync_pm_data(sql)
                print("{0}日pm天粒度数据同步成功".format(not_upload_date_list[i]))
            except Exception as e:
                print("pm同步异常，异常原因{0}".format(e))
        else:
            print("{0}日数据无效，数据同步失败，需核验数据质量".format(not_upload_date_list[i]))

if __name__ == '__main__':
    days_num=${delay_days}
    date_list, date_part = get_date_list(days_num)
    upload_date_list = get_upload_date_list(date_part)
    not_upload_date_list = get_not_upload_date_list(date_list, upload_date_list)
    print("未同步pm数据时间清单 {0}".format(not_upload_date_list))
    if len(not_upload_date_list):
        data_process(not_upload_date_list)
    else:
        print("未同步数据清单为空，所有数据均已同步")
