import trino
from trino import transaction
import pandas as pd
import psycopg2
import datetime as dt
from io import StringIO
from sqlalchemy import create_engine


def get_pm_data(sql):
    conn = trino.dbapi.connect(
        host = '133.160.191.113',
        port = 32470,
        user = 'root',
        catalog = 'hive',
        schema = 'cnio',
        isolation_level = transaction.IsolationLevel.READ_COMMITTED
    )
    cursor = conn.cursor()
    cursor.execute(sql)
    data = cursor.fetchall()
    if len(data):
        data = pd.DataFrame(data)
        colname=[]
        # 获取表头的描述信息，定义一个数组，加上表头信息
        des = cursor.description

        for item in des:
            colname.append(item[0])
        data.columns=colname
        print("pm数据读取成功")
    else:
        print("pm数据读取失败")

    cursor.close()
    conn.close()
    return data


def get_time(hour_num):
    now_time = dt.datetime.now()
    previous_time = now_time + dt.timedelta(hours=hour_num)
    previous_date = previous_time.strftime('%Y%m%d')
    previous_time = previous_time.strftime('%H')
    return previous_date,previous_time

def get_time_list(days_num):
    now_time = dt.datetime.now()
    today_date = now_time.strftime('%Y%m%d')
    min_time = now_time + dt.timedelta(days=days_num)
    min_date = min_time.strftime('%Y%m%d')
    min_time = dt.datetime.strptime(min_date, '%Y%m%d')

    time_list = []
    while now_time >= min_time:
        temp_hour = now_time.strftime('%H')
        temp_date = now_time.strftime('%Y%m%d')
        time_list.append([temp_date, temp_hour])
        now_time = now_time + dt.timedelta(hours=-1)

    date_list = (int(min_date), int(today_date))
    return date_list, time_list

def upload_hour_list(date_list):
    # 寤虹珛杩炴帴
    conn = psycopg2.connect(host="133.160.191.111", user="expert-system", password="YJY_exp#exp502",
                            database="expert-system", port="5432")
    cur = conn.cursor()
    sql = """
        select
        distinct cast(hourtime as varchar) as hourtime
    from
        searching.pm_4g_zdy_hour pgzh 
    where
        daytime  between %s and %s
    """ % date_list
    cur.execute(sql)
    data = cur.fetchall()

    if len(data) != 0:
        data = pd.DataFrame(data)
        data.columns = ["hourtime"]
        data = data["hourtime"].tolist()
        print("数据时间读取完毕")

    else:
        print("数据时间读取失败")
        data = []

    print("upload hour {0}".format(data))
    cur.close()
    conn.close()
    return data

def unload_hour(time_list, load_data_hour):
    unload_hour_list = []
    for i in range(len(time_list)):
        print("time_list {0}".format(time_list[i]))
        if time_list[i][0] + time_list[i][1] in load_data_hour:
            continue
        else:
            unload_hour_list.append(time_list[i])
    return unload_hour_list


def unload_hour(time_list, load_data_hour):
    unload_hour_list = []
    for i in range(len(time_list)):
        print("time_list {0}".format(time_list[i]))
        if time_list[i][0] + time_list[i][1] in load_data_hour:
            continue
        else:
            unload_hour_list.append(time_list[i])
    return unload_hour_list

def postgre_expert(data1):
    engine = create_engine('postgresql://expert-system:YJY_exp#exp502@133.160.191.111:5432/expert-system')
    data1.to_sql("pm_4g_zdy_hour",engine,schema="searching",  index=False, if_exists='append')

def data_upload(data):
    output = StringIO()
    data.to_csv(output, sep='\r', index=False, header=False)
    output1 = output.getvalue()
    conn = psycopg2.connect(host="133.160.191.111", user="expert-system", password="YJY_exp#exp502",
                            database="expert-system", port="5432",options="-c search_path=searching")
    cur = conn.cursor()
    cur.copy_from(StringIO(output1), "pm_4g_zdy_hour",sep='\r',null='')
    conn.commit()
    cur.close()
    conn.close()
    print("pm_4g_zdy_hour sync: success")

## 获取基站信息
def get_siteinfo():
    conn = psycopg2.connect(host="133.160.191.111", user="expert-system", password="YJY_exp#exp502",
                            database="expert-system", port="5432")
    cur = conn.cursor()

    ## 工参获取
    siteinfo_sql = """
    select
        siteid as oid,
        cellid,
        cellname_z as cell_name
    from
        cnio.siteinfo s
    where
        rattype = '4G'
        and data_date = (
        select
            max(data_date)
        from
            cnio.siteinfo s2 );
    """
    cur.execute(siteinfo_sql)
    siteinfo_data = cur.fetchall()
    if len(siteinfo_data):
        siteinfo_data = pd.DataFrame(siteinfo_data)
        siteinfo_data.columns = ["oid", "cellid","cell_name"]

    print("工参获取完毕")
    return siteinfo_data

def data_process(unload_hour_list):
    for i in range(len(unload_hour_list)):
        sql_hour = r''' select
            coalesce(if(province_id='' or province_id='NULL','-1',province_id),'-1') as province_id,
            coalesce(cast(if(city_id='' or city_id='NULL','-1',city_id) as int),-1)  as city_id,
            coalesce(if(city_name='' or city_name='NULL','-1',city_name),'-1') as city_name,
            cast(start_time as timestamp) as start_time,
            coalesce(cast(if(oid='' or oid='NULL','-1',oid) as int),-1)  as oid,
            coalesce(if(related_gnb='' or related_gnb='NULL','-1',related_gnb),'-1') as related_gnb,
            coalesce(cast(if(cellid='' or cellid='NULL','-1',cellid) as int),-1)  as cellid,
            coalesce(cast(if(vendor_id='' or vendor_id='NULL','-1',vendor_id) as int),-1)  as vendor_id,
            coalesce(if(network_type='' or network_type='NULL','-1',network_type),'-1') as network_type,
            coalesce(cast (if(kpi_0000000001='' or kpi_0000000001='NULL','-1',kpi_0000000001) as double),-1) as kpi_0000000001,
            coalesce(cast (if(kpi_0000000008='' or kpi_0000000008='NULL','-1',kpi_0000000008) as double),-1) as kpi_0000000008,
            coalesce(cast (if(kpi_0000000015='' or kpi_0000000015='NULL','-1',kpi_0000000015) as double),-1) as kpi_0000000015,
            coalesce(cast (if(kpi_0000000021='' or kpi_0000000021='NULL','-1',kpi_0000000021) as double),-1) as kpi_0000000021,
            coalesce(cast (if(kpi_0000000027='' or kpi_0000000027='NULL','-1',kpi_0000000027) as double),-1) as kpi_0000000027,
            coalesce(cast (if(kpi_0000000028='' or kpi_0000000028='NULL','-1',kpi_0000000028) as double),-1) as kpi_0000000028,
            coalesce(cast (if(kpi_0000000029='' or kpi_0000000029='NULL','-1',kpi_0000000029) as double),-1) as kpi_0000000029,
            coalesce(cast (if(kpi_0000000030='' or kpi_0000000030='NULL','-1',kpi_0000000030) as double),-1) as kpi_0000000030,
            coalesce(cast (if(kpi_0000000031='' or kpi_0000000031='NULL','-1',kpi_0000000031) as double),-1) as kpi_0000000031,
            coalesce(cast (if(kpi_0000000032='' or kpi_0000000032='NULL','-1',kpi_0000000032) as double),-1) as kpi_0000000032,
            coalesce(cast (if(kpi_0000000033='' or kpi_0000000033='NULL','-1',kpi_0000000033) as double),-1) as kpi_0000000033,
            coalesce(cast (if(kpi_0000000034='' or kpi_0000000034='NULL','-1',kpi_0000000034) as double),-1) as kpi_0000000034,
            coalesce(cast (if(kpi_0000000035='' or kpi_0000000035='NULL','-1',kpi_0000000035) as double),-1) as kpi_0000000035,
            coalesce(cast (if(kpi_0000000036='' or kpi_0000000036='NULL','-1',kpi_0000000036) as double),-1) as kpi_0000000036,
            coalesce(cast (if(kpi_0000000037='' or kpi_0000000037='NULL','-1',kpi_0000000037) as double),-1) as kpi_0000000037,
            coalesce(cast (if(kpi_0000000038='' or kpi_0000000038='NULL','-1',kpi_0000000038) as double),-1) as kpi_0000000038,
            coalesce(cast (if(kpi_0000000039='' or kpi_0000000039='NULL','-1',kpi_0000000039) as double),-1) as kpi_0000000039,
            coalesce(cast (if(kpi_0000000040='' or kpi_0000000040='NULL','-1',kpi_0000000040) as double),-1) as kpi_0000000040,
            coalesce(cast (if(kpi_0000000041='' or kpi_0000000041='NULL','-1',kpi_0000000041) as double),-1) as kpi_0000000041,
            coalesce(cast (if(kpi_0000000042='' or kpi_0000000042='NULL','-1',kpi_0000000042) as double),-1) as kpi_0000000042,
            coalesce(cast (if(kpi_0000000043='' or kpi_0000000043='NULL','-1',kpi_0000000043) as double),-1) as kpi_0000000043,
            coalesce(cast (if(kpi_0000000044='' or kpi_0000000044='NULL','-1',kpi_0000000044) as double),-1) as kpi_0000000044,
            coalesce(cast (if(kpi_0000000045='' or kpi_0000000045='NULL','-1',kpi_0000000045) as double),-1) as kpi_0000000045,
            coalesce(cast (if(kpi_0000000046='' or kpi_0000000046='NULL','-1',kpi_0000000046) as double),-1) as kpi_0000000046,
            coalesce(cast (if(kpi_0000000047='' or kpi_0000000047='NULL','-1',kpi_0000000047) as double),-1) as kpi_0000000047,
            coalesce(cast (if(kpi_0000000047_pucch='' or kpi_0000000047_pucch='NULL','-1',kpi_0000000047_pucch) as double),-1) as kpi_0000000047_pucch,
            coalesce(cast (if(kpi_0000000047_srs='' or kpi_0000000047_srs='NULL','-1',kpi_0000000047_srs) as double),-1) as kpi_0000000047_srs,
            coalesce(cast (if(kpi_0000000047_userspace='' or kpi_0000000047_userspace='NULL','-1',kpi_0000000047_userspace) as double),-1) as kpi_0000000047_userspace,
            coalesce(cast (if(kpi_0000000048='' or kpi_0000000048='NULL','-1',kpi_0000000048) as double),-1) as kpi_0000000048,
            coalesce(cast (if(kpi_0000000049='' or kpi_0000000049='NULL','-1',kpi_0000000049) as double),-1) as kpi_0000000049,
            coalesce(cast (if(kpi_0000000050='' or kpi_0000000050='NULL','-1',kpi_0000000050) as double),-1) as kpi_0000000050,
            coalesce(cast (if(kpi_0000000051='' or kpi_0000000051='NULL','-1',kpi_0000000051) as double),-1) as kpi_0000000051,
            coalesce(cast (if(kpi_0000000055='' or kpi_0000000055='NULL','-1',kpi_0000000055) as double),-1) as kpi_0000000055,
            coalesce(cast (if(kpi_0000000060='' or kpi_0000000060='NULL','-1',kpi_0000000060) as double),-1) as kpi_0000000060,
            coalesce(cast (if(kpi_0000000061='' or kpi_0000000061='NULL','-1',kpi_0000000061) as double),-1) as kpi_0000000061,
            coalesce(cast (if(kpi_0000000065='' or kpi_0000000065='NULL','-1',kpi_0000000065) as double),-1) as kpi_0000000065,
            coalesce(cast (if(kpi_0000000071='' or kpi_0000000071='NULL','-1',kpi_0000000071) as double),-1) as kpi_0000000071,
            coalesce(cast (if(kpi_0000000075='' or kpi_0000000075='NULL','-1',kpi_0000000075) as double),-1) as kpi_0000000075,
            coalesce(cast (if(kpi_0000000081='' or kpi_0000000081='NULL','-1',kpi_0000000081) as double),-1) as kpi_0000000081,
            coalesce(cast (if(kpi_0000000085='' or kpi_0000000085='NULL','-1',kpi_0000000085) as double),-1) as kpi_0000000085,
            coalesce(cast (if(kpi_0000000090='' or kpi_0000000090='NULL','-1',kpi_0000000090) as double),-1) as kpi_0000000090,
            coalesce(cast (if(kpi_0000000121='' or kpi_0000000121='NULL','-1',kpi_0000000121) as double),-1) as kpi_0000000121,
            coalesce(cast (if(kpi_0000000122='' or kpi_0000000122='NULL','-1',kpi_0000000122) as double),-1) as kpi_0000000122,
            coalesce(cast (if(kpi_0000000123='' or kpi_0000000123='NULL','-1',kpi_0000000123) as double),-1) as kpi_0000000123,
            coalesce(cast (if(kpi_0000000133='' or kpi_0000000133='NULL','-1',kpi_0000000133) as double),-1) as kpi_0000000133,
            coalesce(cast (if(kpi_0000000134='' or kpi_0000000134='NULL','-1',kpi_0000000134) as double),-1) as kpi_0000000134,
            coalesce(cast (if(kpi_0000000138='' or kpi_0000000138='NULL','-1',kpi_0000000138) as double),-1) as kpi_0000000138,
            coalesce(cast (if(kpi_0000000143='' or kpi_0000000143='NULL','-1',kpi_0000000143) as double),-1) as kpi_0000000143,
            coalesce(cast (if(kpi_0000000144='' or kpi_0000000144='NULL','-1',kpi_0000000144) as double),-1) as kpi_0000000144,
            coalesce(cast (if(kpi_0000000148='' or kpi_0000000148='NULL','-1',kpi_0000000148) as double),-1) as kpi_0000000148,
            coalesce(cast (if(kpi_0000000153='' or kpi_0000000153='NULL','-1',kpi_0000000153) as double),-1) as kpi_0000000153,
            coalesce(cast (if(kpi_0000000153_v='' or kpi_0000000153_v='NULL','-1',kpi_0000000153_v) as double),-1) as kpi_0000000153_v,
            coalesce(cast (if(kpi_0000000154='' or kpi_0000000154='NULL','-1',kpi_0000000154) as double),-1) as kpi_0000000154,
            coalesce(cast (if(kpi_0000000155='' or kpi_0000000155='NULL','-1',kpi_0000000155) as double),-1) as kpi_0000000155,
            coalesce(cast (if(kpi_0000000156='' or kpi_0000000156='NULL','-1',kpi_0000000156) as double),-1) as kpi_0000000156,
            coalesce(cast (if(kpi_0000000157='' or kpi_0000000157='NULL','-1',kpi_0000000157) as double),-1) as kpi_0000000157,
            coalesce(cast (if(kpi_0000000158='' or kpi_0000000158='NULL','-1',kpi_0000000158) as double),-1) as kpi_0000000158,
            coalesce(cast (if(kpi_0000000158_v='' or kpi_0000000158_v='NULL','-1',kpi_0000000158_v) as double),-1) as kpi_0000000158_v,
            coalesce(cast (if(kpi_0000000159='' or kpi_0000000159='NULL','-1',kpi_0000000159) as double),-1) as kpi_0000000159,
            coalesce(cast (if(kpi_0000000160='' or kpi_0000000160='NULL','-1',kpi_0000000160) as double),-1) as kpi_0000000160,
            coalesce(cast (if(kpi_0000000161='' or kpi_0000000161='NULL','-1',kpi_0000000161) as double),-1) as kpi_0000000161,
            coalesce(cast (if(kpi_0000000162='' or kpi_0000000162='NULL','-1',kpi_0000000162) as double),-1) as kpi_0000000162,
            coalesce(cast (if(kpi_0000000163='' or kpi_0000000163='NULL','-1',kpi_0000000163) as double),-1) as kpi_0000000163,
            coalesce(cast (if(kpi_0000000164='' or kpi_0000000164='NULL','-1',kpi_0000000164) as double),-1) as kpi_0000000164,
            coalesce(cast (if(kpi_0000000165='' or kpi_0000000165='NULL','-1',kpi_0000000165) as double),-1) as kpi_0000000165,
            coalesce(cast (if(kpi_0000000166='' or kpi_0000000166='NULL','-1',kpi_0000000166) as double),-1) as kpi_0000000166,
            coalesce(cast (if(kpi_0000000167='' or kpi_0000000167='NULL','-1',kpi_0000000167) as double),-1) as kpi_0000000167,
            coalesce(cast (if(kpi_0000000168='' or kpi_0000000168='NULL','-1',kpi_0000000168) as double),-1) as kpi_0000000168,
            coalesce(cast (if(kpi_0000000169='' or kpi_0000000169='NULL','-1',kpi_0000000169) as double),-1) as kpi_0000000169,
            coalesce(cast (if(kpi_0000000170='' or kpi_0000000170='NULL','-1',kpi_0000000170) as double),-1) as kpi_0000000170,
            coalesce(cast (if(kpi_0000000171='' or kpi_0000000171='NULL','-1',kpi_0000000171) as double),-1) as kpi_0000000171,
            coalesce(cast (if(kpi_0000000172='' or kpi_0000000172='NULL','-1',kpi_0000000172) as double),-1) as kpi_0000000172,
            coalesce(cast (if(kpi_0000000176='' or kpi_0000000176='NULL','-1',kpi_0000000176) as double),-1) as kpi_0000000176,
            coalesce(cast (if(kpi_0000000181='' or kpi_0000000181='NULL','-1',kpi_0000000181) as double),-1) as kpi_0000000181,
            coalesce(cast (if(kpi_0000000181_hofailure='' or kpi_0000000181_hofailure='NULL','-1',kpi_0000000181_hofailure) as double),-1) as kpi_0000000181_hofailure,
            coalesce(cast (if(kpi_0000000182='' or kpi_0000000182='NULL','-1',kpi_0000000182) as double),-1) as kpi_0000000182,
            coalesce(cast (if(kpi_0000000186='' or kpi_0000000186='NULL','-1',kpi_0000000186) as double),-1) as kpi_0000000186,
            coalesce(cast (if(kpi_0000000191='' or kpi_0000000191='NULL','-1',kpi_0000000191) as double),-1) as kpi_0000000191,
            coalesce(cast (if(kpi_0000000192='' or kpi_0000000192='NULL','-1',kpi_0000000192) as double),-1) as kpi_0000000192,
            coalesce(cast (if(kpi_0000000193='' or kpi_0000000193='NULL','-1',kpi_0000000193) as double),-1) as kpi_0000000193,
            coalesce(cast (if(kpi_0000000194='' or kpi_0000000194='NULL','-1',kpi_0000000194) as double),-1) as kpi_0000000194,
            coalesce(cast (if(kpi_0000000195='' or kpi_0000000195='NULL','-1',kpi_0000000195) as double),-1) as kpi_0000000195,
            coalesce(cast (if(kpi_0000000196='' or kpi_0000000196='NULL','-1',kpi_0000000196) as double),-1) as kpi_0000000196,
            coalesce(cast (if(kpi_0000000197='' or kpi_0000000197='NULL','-1',kpi_0000000197) as double),-1) as kpi_0000000197,
            coalesce(cast (if(kpi_0000000198='' or kpi_0000000198='NULL','-1',kpi_0000000198) as double),-1) as kpi_0000000198,
            coalesce(cast (if(kpi_0000000199='' or kpi_0000000199='NULL','-1',kpi_0000000199) as double),-1) as kpi_0000000199,
            coalesce(cast (if(kpi_0000000200='' or kpi_0000000200='NULL','-1',kpi_0000000200) as double),-1) as kpi_0000000200,
            coalesce(cast (if(kpi_0000000201='' or kpi_0000000201='NULL','-1',kpi_0000000201) as double),-1) as kpi_0000000201,
            coalesce(cast (if(kpi_0000000205='' or kpi_0000000205='NULL','-1',kpi_0000000205) as double),-1) as kpi_0000000205,
            coalesce(cast (if(kpi_0000000210='' or kpi_0000000210='NULL','-1',kpi_0000000210) as double),-1) as kpi_0000000210,
            coalesce(cast (if(kpi_0000000220='' or kpi_0000000220='NULL','-1',kpi_0000000220) as double),-1) as kpi_0000000220,
            coalesce(cast (if(kpi_0000000220_rnl='' or kpi_0000000220_rnl='NULL','-1',kpi_0000000220_rnl) as double),-1) as kpi_0000000220_rnl,
            coalesce(cast (if(kpi_0000000221='' or kpi_0000000221='NULL','-1',kpi_0000000221) as double),-1) as kpi_0000000221,
            coalesce(cast (if(kpi_0000000222='' or kpi_0000000222='NULL','-1',kpi_0000000222) as double),-1) as kpi_0000000222,
            coalesce(cast (if(kpi_0000000223='' or kpi_0000000223='NULL','-1',kpi_0000000223) as double),-1) as kpi_0000000223,
            coalesce(cast (if(kpi_0000000224='' or kpi_0000000224='NULL','-1',kpi_0000000224) as double),-1) as kpi_0000000224,
            coalesce(cast (if(kpi_0000000225='' or kpi_0000000225='NULL','-1',kpi_0000000225) as double),-1) as kpi_0000000225,
            coalesce(cast (if(kpi_0000000226='' or kpi_0000000226='NULL','-1',kpi_0000000226) as double),-1) as kpi_0000000226,
            coalesce(cast (if(kpi_0000000227='' or kpi_0000000227='NULL','-1',kpi_0000000227) as double),-1) as kpi_0000000227,
            coalesce(cast (if(kpi_0000000228='' or kpi_0000000228='NULL','-1',kpi_0000000228) as double),-1) as kpi_0000000228,
            coalesce(cast (if(kpi_0000000228_v='' or kpi_0000000228_v='NULL','-1',kpi_0000000228_v) as double),-1) as kpi_0000000228_v,
            coalesce(cast (if(kpi_0000000229='' or kpi_0000000229='NULL','-1',kpi_0000000229) as double),-1) as kpi_0000000229,
            coalesce(cast (if(kpi_0000000229_v='' or kpi_0000000229_v='NULL','-1',kpi_0000000229_v) as double),-1) as kpi_0000000229_v,
            coalesce(cast (if(kpi_0000000230='' or kpi_0000000230='NULL','-1',kpi_0000000230) as double),-1) as kpi_0000000230,
            coalesce(cast (if(kpi_0000000230_v='' or kpi_0000000230_v='NULL','-1',kpi_0000000230_v) as double),-1) as kpi_0000000230_v,
            coalesce(cast (if(kpi_0000000231='' or kpi_0000000231='NULL','-1',kpi_0000000231) as double),-1) as kpi_0000000231,
            coalesce(cast (if(kpi_0000000231_v='' or kpi_0000000231_v='NULL','-1',kpi_0000000231_v) as double),-1) as kpi_0000000231_v,
            coalesce(cast (if(kpi_0000000232='' or kpi_0000000232='NULL','-1',kpi_0000000232) as double),-1) as kpi_0000000232,
            coalesce(cast (if(kpi_0000000233='' or kpi_0000000233='NULL','-1',kpi_0000000233) as double),-1) as kpi_0000000233,
            coalesce(cast (if(kpi_0000000237='' or kpi_0000000237='NULL','-1',kpi_0000000237) as double),-1) as kpi_0000000237,
            coalesce(cast (if(kpi_0000000242='' or kpi_0000000242='NULL','-1',kpi_0000000242) as double),-1) as kpi_0000000242,
            coalesce(cast (if(kpi_0000000243='' or kpi_0000000243='NULL','-1',kpi_0000000243) as double),-1) as kpi_0000000243,
            coalesce(cast (if(kpi_0000000247='' or kpi_0000000247='NULL','-1',kpi_0000000247) as double),-1) as kpi_0000000247,
            coalesce(cast (if(kpi_0000000252='' or kpi_0000000252='NULL','-1',kpi_0000000252) as double),-1) as kpi_0000000252,
            coalesce(cast (if(kpi_0000000253='' or kpi_0000000253='NULL','-1',kpi_0000000253) as double),-1) as kpi_0000000253,
            coalesce(cast (if(kpi_0000000257='' or kpi_0000000257='NULL','-1',kpi_0000000257) as double),-1) as kpi_0000000257,
            coalesce(cast (if(kpi_0000000262='' or kpi_0000000262='NULL','-1',kpi_0000000262) as double),-1) as kpi_0000000262,
            coalesce(cast (if(kpi_0000000263='' or kpi_0000000263='NULL','-1',kpi_0000000263) as double),-1) as kpi_0000000263,
            coalesce(cast (if(kpi_0000000267='' or kpi_0000000267='NULL','-1',kpi_0000000267) as double),-1) as kpi_0000000267,
            coalesce(cast (if(kpi_0000000272='' or kpi_0000000272='NULL','-1',kpi_0000000272) as double),-1) as kpi_0000000272,
            coalesce(cast (if(kpi_0000000273='' or kpi_0000000273='NULL','-1',kpi_0000000273) as double),-1) as kpi_0000000273,
            coalesce(cast (if(kpi_0000000274='' or kpi_0000000274='NULL','-1',kpi_0000000274) as double),-1) as kpi_0000000274,
            coalesce(cast (if(kpi_0000000275='' or kpi_0000000275='NULL','-1',kpi_0000000275) as double),-1) as kpi_0000000275,
            coalesce(cast (if(kpi_0000000276='' or kpi_0000000276='NULL','-1',kpi_0000000276) as double),-1) as kpi_0000000276,
            coalesce(cast (if(kpi_0000000277='' or kpi_0000000277='NULL','-1',kpi_0000000277) as double),-1) as kpi_0000000277,
            coalesce(cast (if(kpi_0000000278='' or kpi_0000000278='NULL','-1',kpi_0000000278) as double),-1) as kpi_0000000278,
            coalesce(cast (if(kpi_0000000279='' or kpi_0000000279='NULL','-1',kpi_0000000279) as double),-1) as kpi_0000000279,
            coalesce(cast (if(kpi_0000000280='' or kpi_0000000280='NULL','-1',kpi_0000000280) as double),-1) as kpi_0000000280,
            coalesce(cast (if(kpi_0000000281='' or kpi_0000000281='NULL','-1',kpi_0000000281) as double),-1) as kpi_0000000281,
            coalesce(cast (if(kpi_0000000282='' or kpi_0000000282='NULL','-1',kpi_0000000282) as double),-1) as kpi_0000000282,
            coalesce(cast (if(kpi_0000000283='' or kpi_0000000283='NULL','-1',kpi_0000000283) as double),-1) as kpi_0000000283,
            coalesce(cast (if(kpi_0000000284='' or kpi_0000000284='NULL','-1',kpi_0000000284) as double),-1) as kpi_0000000284,
            coalesce(cast (if(kpi_0000000285='' or kpi_0000000285='NULL','-1',kpi_0000000285) as double),-1) as kpi_0000000285,
            coalesce(cast (if(kpi_0000000286='' or kpi_0000000286='NULL','-1',kpi_0000000286) as double),-1) as kpi_0000000286,
            coalesce(cast (if(kpi_0000000287='' or kpi_0000000287='NULL','-1',kpi_0000000287) as double),-1) as kpi_0000000287,
            coalesce(cast (if(kpi_0000000288='' or kpi_0000000288='NULL','-1',kpi_0000000288) as double),-1) as kpi_0000000288,
            coalesce(cast (if(kpi_0000000289='' or kpi_0000000289='NULL','-1',kpi_0000000289) as double),-1) as kpi_0000000289,
            coalesce(cast (if(kpi_0000000290='' or kpi_0000000290='NULL','-1',kpi_0000000290) as double),-1) as kpi_0000000290,
            coalesce(cast (if(kpi_0000000291='' or kpi_0000000291='NULL','-1',kpi_0000000291) as double),-1) as kpi_0000000291,
            coalesce(cast (if(kpi_0000000292='' or kpi_0000000292='NULL','-1',kpi_0000000292) as double),-1) as kpi_0000000292,
            coalesce(cast (if(kpi_0000000293='' or kpi_0000000293='NULL','-1',kpi_0000000293) as double),-1) as kpi_0000000293,
            coalesce(cast (if(kpi_0000000294='' or kpi_0000000294='NULL','-1',kpi_0000000294) as double),-1) as kpi_0000000294,
            coalesce(cast (if(kpi_0000000295='' or kpi_0000000295='NULL','-1',kpi_0000000295) as double),-1) as kpi_0000000295,
            coalesce(cast (if(kpi_0000000296='' or kpi_0000000296='NULL','-1',kpi_0000000296) as double),-1) as kpi_0000000296,
            coalesce(cast (if(kpi_0000000297='' or kpi_0000000297='NULL','-1',kpi_0000000297) as double),-1) as kpi_0000000297,
            coalesce(cast (if(kpi_0000000298='' or kpi_0000000298='NULL','-1',kpi_0000000298) as double),-1) as kpi_0000000298,
            coalesce(cast (if(kpi_0000000299='' or kpi_0000000299='NULL','-1',kpi_0000000299) as double),-1) as kpi_0000000299,
            coalesce(cast (if(kpi_0000000300='' or kpi_0000000300='NULL','-1',kpi_0000000300) as double),-1) as kpi_0000000300,
            coalesce(cast (if(kpi_0000000301='' or kpi_0000000301='NULL','-1',kpi_0000000301) as double),-1) as kpi_0000000301,
            coalesce(cast (if(kpi_0000000302='' or kpi_0000000302='NULL','-1',kpi_0000000302) as double),-1) as kpi_0000000302,
            coalesce(cast (if(kpi_0000000303='' or kpi_0000000303='NULL','-1',kpi_0000000303) as double),-1) as kpi_0000000303,
            coalesce(cast (if(kpi_0000000304='' or kpi_0000000304='NULL','-1',kpi_0000000304) as double),-1) as kpi_0000000304,
            coalesce(cast (if(kpi_0000000305='' or kpi_0000000305='NULL','-1',kpi_0000000305) as double),-1) as kpi_0000000305,
            coalesce(cast (if(kpi_0000000306='' or kpi_0000000306='NULL','-1',kpi_0000000306) as double),-1) as kpi_0000000306,
            coalesce(cast (if(kpi_0000000307='' or kpi_0000000307='NULL','-1',kpi_0000000307) as double),-1) as kpi_0000000307,
            coalesce(cast (if(kpi_0000000308='' or kpi_0000000308='NULL','-1',kpi_0000000308) as double),-1) as kpi_0000000308,
            coalesce(cast (if(kpi_0000000309='' or kpi_0000000309='NULL','-1',kpi_0000000309) as double),-1) as kpi_0000000309,
            coalesce(cast (if(kpi_0000000310='' or kpi_0000000310='NULL','-1',kpi_0000000310) as double),-1) as kpi_0000000310,
            coalesce(cast (if(kpi_0000000311='' or kpi_0000000311='NULL','-1',kpi_0000000311) as double),-1) as kpi_0000000311,
            coalesce(cast (if(kpi_0000000312='' or kpi_0000000312='NULL','-1',kpi_0000000312) as double),-1) as kpi_0000000312,
            coalesce(cast (if(kpi_0000000313='' or kpi_0000000313='NULL','-1',kpi_0000000313) as double),-1) as kpi_0000000313,
            coalesce(cast (if(kpi_0000000314='' or kpi_0000000314='NULL','-1',kpi_0000000314) as double),-1) as kpi_0000000314,
            coalesce(cast (if(kpi_0000000315='' or kpi_0000000315='NULL','-1',kpi_0000000315) as double),-1) as kpi_0000000315,
            coalesce(cast (if(kpi_0000000316='' or kpi_0000000316='NULL','-1',kpi_0000000316) as double),-1) as kpi_0000000316,
            coalesce(cast (if(kpi_0000000317='' or kpi_0000000317='NULL','-1',kpi_0000000317) as double),-1) as kpi_0000000317,
            coalesce(cast (if(kpi_0000000318='' or kpi_0000000318='NULL','-1',kpi_0000000318) as double),-1) as kpi_0000000318,
            coalesce(cast (if(kpi_0000000319='' or kpi_0000000319='NULL','-1',kpi_0000000319) as double),-1) as kpi_0000000319,
            coalesce(cast (if(kpi_0000000320='' or kpi_0000000320='NULL','-1',kpi_0000000320) as double),-1) as kpi_0000000320,
            coalesce(cast (if(kpi_0000000321='' or kpi_0000000321='NULL','-1',kpi_0000000321) as double),-1) as kpi_0000000321,
            coalesce(cast (if(kpi_0000000322='' or kpi_0000000322='NULL','-1',kpi_0000000322) as double),-1) as kpi_0000000322,
            coalesce(cast (if(kpi_0000000323='' or kpi_0000000323='NULL','-1',kpi_0000000323) as double),-1) as kpi_0000000323,
            coalesce(cast (if(kpi_0000000324='' or kpi_0000000324='NULL','-1',kpi_0000000324) as double),-1) as kpi_0000000324,
            coalesce(cast (if(kpi_0000000325='' or kpi_0000000325='NULL','-1',kpi_0000000325) as double),-1) as kpi_0000000325,
            coalesce(cast (if(kpi_0000000326='' or kpi_0000000326='NULL','-1',kpi_0000000326) as double),-1) as kpi_0000000326,
            coalesce(cast (if(kpi_0000000327='' or kpi_0000000327='NULL','-1',kpi_0000000327) as double),-1) as kpi_0000000327,
            coalesce(cast (if(kpi_0000000328='' or kpi_0000000328='NULL','-1',kpi_0000000328) as double),-1) as kpi_0000000328,
            coalesce(cast (if(kpi_0000000329='' or kpi_0000000329='NULL','-1',kpi_0000000329) as double),-1) as kpi_0000000329,
            coalesce(cast (if(kpi_0000000330='' or kpi_0000000330='NULL','-1',kpi_0000000330) as double),-1) as kpi_0000000330,
            coalesce(cast (if(kpi_0000000331='' or kpi_0000000331='NULL','-1',kpi_0000000331) as double),-1) as kpi_0000000331,
            coalesce(cast (if(kpi_0000000332='' or kpi_0000000332='NULL','-1',kpi_0000000332) as double),-1) as kpi_0000000332,
            coalesce(cast (if(kpi_0000000336='' or kpi_0000000336='NULL','-1',kpi_0000000336) as double),-1) as kpi_0000000336,
            coalesce(cast (if(kpi_0000000342='' or kpi_0000000342='NULL','-1',kpi_0000000342) as double),-1) as kpi_0000000342,
            coalesce(cast (if(kpi_0000000346='' or kpi_0000000346='NULL','-1',kpi_0000000346) as double),-1) as kpi_0000000346,
            coalesce(cast (if(kpi_0000000352='' or kpi_0000000352='NULL','-1',kpi_0000000352) as double),-1) as kpi_0000000352,
            coalesce(cast (if(kpi_0000000356='' or kpi_0000000356='NULL','-1',kpi_0000000356) as double),-1) as kpi_0000000356,
            coalesce(cast (if(kpi_0000000362='' or kpi_0000000362='NULL','-1',kpi_0000000362) as double),-1) as kpi_0000000362,
            coalesce(cast (if(kpi_0000000366='' or kpi_0000000366='NULL','-1',kpi_0000000366) as double),-1) as kpi_0000000366,
            coalesce(cast (if(kpi_0000000372='' or kpi_0000000372='NULL','-1',kpi_0000000372) as double),-1) as kpi_0000000372,
            coalesce(cast (if(kpi_0000000376='' or kpi_0000000376='NULL','-1',kpi_0000000376) as double),-1) as kpi_0000000376,
            coalesce(cast (if(kpi_0000000381='' or kpi_0000000381='NULL','-1',kpi_0000000381) as double),-1) as kpi_0000000381,
            coalesce(cast (if(kpi_0000000383='' or kpi_0000000383='NULL','-1',kpi_0000000383) as double),-1) as kpi_0000000383,
            coalesce(cast (if(kpi_0000000385='' or kpi_0000000385='NULL','-1',kpi_0000000385) as double),-1) as kpi_0000000385,
            coalesce(cast (if(kpi_0000000386='' or kpi_0000000386='NULL','-1',kpi_0000000386) as double),-1) as kpi_0000000386,
            coalesce(cast (if(kpi_0000000390='' or kpi_0000000390='NULL','-1',kpi_0000000390) as double),-1) as kpi_0000000390,
            coalesce(cast (if(kpi_0000000395='' or kpi_0000000395='NULL','-1',kpi_0000000395) as double),-1) as kpi_0000000395,
            coalesce(cast (if(kpi_0000000395_1='' or kpi_0000000395_1='NULL','-1',kpi_0000000395_1) as double),-1) as kpi_0000000395_1,
            coalesce(cast (if(kpi_0000000395_5='' or kpi_0000000395_5='NULL','-1',kpi_0000000395_5) as double),-1) as kpi_0000000395_5,
            coalesce(cast (if(kpi_0000000396='' or kpi_0000000396='NULL','-1',kpi_0000000396) as double),-1) as kpi_0000000396,
            coalesce(cast (if(kpi_0000000396_1='' or kpi_0000000396_1='NULL','-1',kpi_0000000396_1) as double),-1) as kpi_0000000396_1,
            coalesce(cast (if(kpi_0000000396_5='' or kpi_0000000396_5='NULL','-1',kpi_0000000396_5) as double),-1) as kpi_0000000396_5,
            coalesce(cast (if(kpi_0000000397='' or kpi_0000000397='NULL','-1',kpi_0000000397) as double),-1) as kpi_0000000397,
            coalesce(cast (if(kpi_0000000401='' or kpi_0000000401='NULL','-1',kpi_0000000401) as double),-1) as kpi_0000000401,
            coalesce(cast (if(kpi_0000000406='' or kpi_0000000406='NULL','-1',kpi_0000000406) as double),-1) as kpi_0000000406,
            coalesce(cast (if(kpi_0000000406_1='' or kpi_0000000406_1='NULL','-1',kpi_0000000406_1) as double),-1) as kpi_0000000406_1,
            coalesce(cast (if(kpi_0000000406_5='' or kpi_0000000406_5='NULL','-1',kpi_0000000406_5) as double),-1) as kpi_0000000406_5,
            coalesce(cast (if(kpi_0000000475='' or kpi_0000000475='NULL','-1',kpi_0000000475) as double),-1) as kpi_0000000475,
            coalesce(cast (if(kpi_0000000476='' or kpi_0000000476='NULL','-1',kpi_0000000476) as double),-1) as kpi_0000000476,
            coalesce(cast (if(kpi_0000000477='' or kpi_0000000477='NULL','-1',kpi_0000000477) as double),-1) as kpi_0000000477,
            coalesce(cast (if(kpi_0000000480='' or kpi_0000000480='NULL','-1',kpi_0000000480) as double),-1) as kpi_0000000480,
            coalesce(cast (if(kpi_0000000483='' or kpi_0000000483='NULL','-1',kpi_0000000483) as double),-1) as kpi_0000000483,
            coalesce(cast (if(kpi_0000000486='' or kpi_0000000486='NULL','-1',kpi_0000000486) as double),-1) as kpi_0000000486,
            coalesce(cast (if(kpi_0000000487='' or kpi_0000000487='NULL','-1',kpi_0000000487) as double),-1) as kpi_0000000487,
            coalesce(cast (if(kpi_0000000490='' or kpi_0000000490='NULL','-1',kpi_0000000490) as double),-1) as kpi_0000000490,
            coalesce(cast (if(kpi_0000000493='' or kpi_0000000493='NULL','-1',kpi_0000000493) as double),-1) as kpi_0000000493,
            coalesce(cast (if(kpi_0000000496='' or kpi_0000000496='NULL','-1',kpi_0000000496) as double),-1) as kpi_0000000496,
            coalesce(cast (if(kpi_0000000497='' or kpi_0000000497='NULL','-1',kpi_0000000497) as double),-1) as kpi_0000000497,
            coalesce(cast (if(kpi_0000000498='' or kpi_0000000498='NULL','-1',kpi_0000000498) as double),-1) as kpi_0000000498,
            coalesce(cast (if(kpi_0000000508='' or kpi_0000000508='NULL','-1',kpi_0000000508) as double),-1) as kpi_0000000508,
            coalesce(cast (if(kpi_0000000518='' or kpi_0000000518='NULL','-1',kpi_0000000518) as double),-1) as kpi_0000000518,
            coalesce(cast (if(kpi_0000000518_1='' or kpi_0000000518_1='NULL','-1',kpi_0000000518_1) as double),-1) as kpi_0000000518_1,
            coalesce(cast (if(kpi_0000000518_5='' or kpi_0000000518_5='NULL','-1',kpi_0000000518_5) as double),-1) as kpi_0000000518_5,
            coalesce(cast (if(kpi_0000000519='' or kpi_0000000519='NULL','-1',kpi_0000000519) as double),-1) as kpi_0000000519,
            coalesce(cast (if(kpi_0000000522='' or kpi_0000000522='NULL','-1',kpi_0000000522) as double),-1) as kpi_0000000522,
            coalesce(cast (if(kpi_0000000523='' or kpi_0000000523='NULL','-1',kpi_0000000523) as double),-1) as kpi_0000000523,
            coalesce(cast (if(kpi_0000000524='' or kpi_0000000524='NULL','-1',kpi_0000000524) as double),-1) as kpi_0000000524,
            coalesce(cast (if(kpi_0000000543='' or kpi_0000000543='NULL','-1',kpi_0000000543) as double),-1) as kpi_0000000543,
            coalesce(cast (if(kpi_0000000543_1='' or kpi_0000000543_1='NULL','-1',kpi_0000000543_1) as double),-1) as kpi_0000000543_1,
            coalesce(cast (if(kpi_0000000543_5='' or kpi_0000000543_5='NULL','-1',kpi_0000000543_5) as double),-1) as kpi_0000000543_5,
            coalesce(cast (if(kpi_0000000544='' or kpi_0000000544='NULL','-1',kpi_0000000544) as double),-1) as kpi_0000000544,
            coalesce(cast (if(kpi_0000000545='' or kpi_0000000545='NULL','-1',kpi_0000000545) as double),-1) as kpi_0000000545,
            coalesce(cast (if(kpi_0000000546='' or kpi_0000000546='NULL','-1',kpi_0000000546) as double),-1) as kpi_0000000546,
            coalesce(cast (if(kpi_0000000547='' or kpi_0000000547='NULL','-1',kpi_0000000547) as double),-1) as kpi_0000000547,
            coalesce(cast (if(kpi_0000000547_1='' or kpi_0000000547_1='NULL','-1',kpi_0000000547_1) as double),-1) as kpi_0000000547_1,
            coalesce(cast (if(kpi_0000000547_5='' or kpi_0000000547_5='NULL','-1',kpi_0000000547_5) as double),-1) as kpi_0000000547_5,
            coalesce(cast (if(kpi_0000000551='' or kpi_0000000551='NULL','-1',kpi_0000000551) as double),-1) as kpi_0000000551,
            coalesce(cast (if(kpi_0000000551_k='' or kpi_0000000551_k='NULL','-1',kpi_0000000551_k) as double),-1) as kpi_0000000551_k,
            coalesce(cast (if(kpi_0000000552='' or kpi_0000000552='NULL','-1',kpi_0000000552) as double),-1) as kpi_0000000552,
            coalesce(cast (if(kpi_0000000553='' or kpi_0000000553='NULL','-1',kpi_0000000553) as double),-1) as kpi_0000000553,
            coalesce(cast (if(kpi_0000000554='' or kpi_0000000554='NULL','-1',kpi_0000000554) as double),-1) as kpi_0000000554,
            coalesce(cast (if(kpi_0000000555='' or kpi_0000000555='NULL','-1',kpi_0000000555) as double),-1) as kpi_0000000555,
            coalesce(cast (if(kpi_0000000556='' or kpi_0000000556='NULL','-1',kpi_0000000556) as double),-1) as kpi_0000000556,
            coalesce(cast (if(kpi_0000000557='' or kpi_0000000557='NULL','-1',kpi_0000000557) as double),-1) as kpi_0000000557,
            coalesce(cast (if(kpi_0000000558='' or kpi_0000000558='NULL','-1',kpi_0000000558) as double),-1) as kpi_0000000558,
            coalesce(cast (if(kpi_0000000559='' or kpi_0000000559='NULL','-1',kpi_0000000559) as double),-1) as kpi_0000000559,
            coalesce(cast (if(kpi_0000000560='' or kpi_0000000560='NULL','-1',kpi_0000000560) as double),-1) as kpi_0000000560,
            coalesce(cast (if(kpi_0000000577='' or kpi_0000000577='NULL','-1',kpi_0000000577) as double),-1) as kpi_0000000577,
            coalesce(cast (if(kpi_0000000578='' or kpi_0000000578='NULL','-1',kpi_0000000578) as double),-1) as kpi_0000000578,
            coalesce(cast (if(kpi_0000000579='' or kpi_0000000579='NULL','-1',kpi_0000000579) as double),-1) as kpi_0000000579,
            coalesce(cast (if(kpi_0000000582='' or kpi_0000000582='NULL','-1',kpi_0000000582) as double),-1) as kpi_0000000582,
            coalesce(cast (if(kpi_0000000583='' or kpi_0000000583='NULL','-1',kpi_0000000583) as double),-1) as kpi_0000000583,
            coalesce(cast (if(kpi_0000000584='' or kpi_0000000584='NULL','-1',kpi_0000000584) as double),-1) as kpi_0000000584,
            coalesce(cast (if(kpi_0000000585='' or kpi_0000000585='NULL','-1',kpi_0000000585) as double),-1) as kpi_0000000585,
            coalesce(cast (if(kpi_0000000585_k='' or kpi_0000000585_k='NULL','-1',kpi_0000000585_k) as double),-1) as kpi_0000000585_k,
            coalesce(cast (if(kpi_0000000586='' or kpi_0000000586='NULL','-1',kpi_0000000586) as double),-1) as kpi_0000000586,
            coalesce(cast (if(kpi_0000000587='' or kpi_0000000587='NULL','-1',kpi_0000000587) as double),-1) as kpi_0000000587,
            coalesce(cast (if(kpi_0000000588='' or kpi_0000000588='NULL','-1',kpi_0000000588) as double),-1) as kpi_0000000588,
            coalesce(cast (if(kpi_0000000589='' or kpi_0000000589='NULL','-1',kpi_0000000589) as double),-1) as kpi_0000000589,
            coalesce(cast (if(kpi_0000000590='' or kpi_0000000590='NULL','-1',kpi_0000000590) as double),-1) as kpi_0000000590,
            coalesce(cast (if(kpi_0000000591='' or kpi_0000000591='NULL','-1',kpi_0000000591) as double),-1) as kpi_0000000591,
            coalesce(cast (if(kpi_0000000592='' or kpi_0000000592='NULL','-1',kpi_0000000592) as double),-1) as kpi_0000000592,
            coalesce(cast (if(kpi_0000000595='' or kpi_0000000595='NULL','-1',kpi_0000000595) as double),-1) as kpi_0000000595,
            coalesce(cast (if(kpi_0000000596='' or kpi_0000000596='NULL','-1',kpi_0000000596) as double),-1) as kpi_0000000596,
            coalesce(cast (if(kpi_0000000597='' or kpi_0000000597='NULL','-1',kpi_0000000597) as double),-1) as kpi_0000000597,
            coalesce(cast (if(kpi_0000000598='' or kpi_0000000598='NULL','-1',kpi_0000000598) as double),-1) as kpi_0000000598,
            coalesce(cast (if(kpi_0000000599='' or kpi_0000000599='NULL','-1',kpi_0000000599) as double),-1) as kpi_0000000599,
            coalesce(cast (if(kpi_0000000600='' or kpi_0000000600='NULL','-1',kpi_0000000600) as double),-1) as kpi_0000000600,
            coalesce(cast (if(kpi_0000000601='' or kpi_0000000601='NULL','-1',kpi_0000000601) as double),-1) as kpi_0000000601,
            coalesce(cast (if(kpi_0000000602='' or kpi_0000000602='NULL','-1',kpi_0000000602) as double),-1) as kpi_0000000602,
            coalesce(cast (if(kpi_0000000603='' or kpi_0000000603='NULL','-1',kpi_0000000603) as double),-1) as kpi_0000000603,
            coalesce(cast (if(kpi_0000000604='' or kpi_0000000604='NULL','-1',kpi_0000000604) as double),-1) as kpi_0000000604,
            coalesce(cast (if(kpi_0000000605='' or kpi_0000000605='NULL','-1',kpi_0000000605) as double),-1) as kpi_0000000605,
            coalesce(cast (if(kpi_0000000606='' or kpi_0000000606='NULL','-1',kpi_0000000606) as double),-1) as kpi_0000000606,
            coalesce(cast (if(kpi_0000000607='' or kpi_0000000607='NULL','-1',kpi_0000000607) as double),-1) as kpi_0000000607,
            coalesce(cast (if(kpi_0000000608='' or kpi_0000000608='NULL','-1',kpi_0000000608) as double),-1) as kpi_0000000608,
            coalesce(cast (if(kpi_0000000609='' or kpi_0000000609='NULL','-1',kpi_0000000609) as double),-1) as kpi_0000000609,
            coalesce(cast (if(kpi_0000000610='' or kpi_0000000610='NULL','-1',kpi_0000000610) as double),-1) as kpi_0000000610,
            coalesce(cast (if(kpi_0000000611='' or kpi_0000000611='NULL','-1',kpi_0000000611) as double),-1) as kpi_0000000611,
            coalesce(cast (if(kpi_0000000612='' or kpi_0000000612='NULL','-1',kpi_0000000612) as double),-1) as kpi_0000000612,
            coalesce(cast (if(kpi_0000000613='' or kpi_0000000613='NULL','-1',kpi_0000000613) as double),-1) as kpi_0000000613,
            coalesce(cast (if(kpi_0000000614='' or kpi_0000000614='NULL','-1',kpi_0000000614) as double),-1) as kpi_0000000614,
            coalesce(cast (if(kpi_0000000615='' or kpi_0000000615='NULL','-1',kpi_0000000615) as double),-1) as kpi_0000000615,
            coalesce(cast (if(kpi_0000000616='' or kpi_0000000616='NULL','-1',kpi_0000000616) as double),-1) as kpi_0000000616,
            coalesce(cast (if(kpi_0000000617='' or kpi_0000000617='NULL','-1',kpi_0000000617) as double),-1) as kpi_0000000617,
            coalesce(cast (if(kpi_0000000617_cu='' or kpi_0000000617_cu='NULL','-1',kpi_0000000617_cu) as double),-1) as kpi_0000000617_cu,
            coalesce(cast (if(kpi_0000000617_du='' or kpi_0000000617_du='NULL','-1',kpi_0000000617_du) as double),-1) as kpi_0000000617_du,
            coalesce(cast (if(kpi_0000000617_uu='' or kpi_0000000617_uu='NULL','-1',kpi_0000000617_uu) as double),-1) as kpi_0000000617_uu,
            coalesce(cast (if(kpi_0000000618='' or kpi_0000000618='NULL','-1',kpi_0000000618) as double),-1) as kpi_0000000618,
            coalesce(cast (if(kpi_0000000619='' or kpi_0000000619='NULL','-1',kpi_0000000619) as double),-1) as kpi_0000000619,
            coalesce(cast (if(kpi_0000000620='' or kpi_0000000620='NULL','-1',kpi_0000000620) as double),-1) as kpi_0000000620,
            coalesce(cast (if(kpi_0000000623='' or kpi_0000000623='NULL','-1',kpi_0000000623) as double),-1) as kpi_0000000623,
            coalesce(cast (if(kpi_0000000624='' or kpi_0000000624='NULL','-1',kpi_0000000624) as double),-1) as kpi_0000000624,
            coalesce(cast (if(kpi_0000000625='' or kpi_0000000625='NULL','-1',kpi_0000000625) as double),-1) as kpi_0000000625,
            coalesce(cast (if(kpi_0000000625_m='' or kpi_0000000625_m='NULL','-1',kpi_0000000625_m) as double),-1) as kpi_0000000625_m,
            coalesce(cast (if(kpi_0000000625_s='' or kpi_0000000625_s='NULL','-1',kpi_0000000625_s) as double),-1) as kpi_0000000625_s,
            coalesce(cast (if(kpi_0000000626='' or kpi_0000000626='NULL','-1',kpi_0000000626) as double),-1) as kpi_0000000626,
            coalesce(cast (if(kpi_0000000627='' or kpi_0000000627='NULL','-1',kpi_0000000627) as double),-1) as kpi_0000000627,
            coalesce(cast (if(kpi_0000000628='' or kpi_0000000628='NULL','-1',kpi_0000000628) as double),-1) as kpi_0000000628,
            coalesce(cast (if(kpi_0000000629='' or kpi_0000000629='NULL','-1',kpi_0000000629) as double),-1) as kpi_0000000629,
            coalesce(cast (if(kpi_0000000630='' or kpi_0000000630='NULL','-1',kpi_0000000630) as double),-1) as kpi_0000000630,
            coalesce(cast (if(kpi_0000000631='' or kpi_0000000631='NULL','-1',kpi_0000000631) as double),-1) as kpi_0000000631,
            coalesce(cast (if(kpi_0000000652='' or kpi_0000000652='NULL','-1',kpi_0000000652) as double),-1) as kpi_0000000652,
            coalesce(cast (if(kpi_0000000653='' or kpi_0000000653='NULL','-1',kpi_0000000653) as double),-1) as kpi_0000000653,
            coalesce(cast (if(kpi_0000000654='' or kpi_0000000654='NULL','-1',kpi_0000000654) as double),-1) as kpi_0000000654,
            coalesce(cast (if(kpi_0000000655='' or kpi_0000000655='NULL','-1',kpi_0000000655) as double),-1) as kpi_0000000655,
            coalesce(cast (if(kpi_0000000684='' or kpi_0000000684='NULL','-1',kpi_0000000684) as double),-1) as kpi_0000000684,
            coalesce(cast (if(kpi_0000000685='' or kpi_0000000685='NULL','-1',kpi_0000000685) as double),-1) as kpi_0000000685,
            coalesce(cast (if(kpi_0000000686='' or kpi_0000000686='NULL','-1',kpi_0000000686) as double),-1) as kpi_0000000686,
            coalesce(cast (if(kpi_0000000687='' or kpi_0000000687='NULL','-1',kpi_0000000687) as double),-1) as kpi_0000000687,
            (case when (if( kpi_0000000411='',0.0,cast (kpi_0000000411 as double))+if( kpi_0000000412='',0.0,cast (kpi_0000000412 as double))+if( kpi_0000000413='',0.0,cast (kpi_0000000413 as double))+if( kpi_0000000414='',0.0,cast (kpi_0000000414 as double))+if( kpi_0000000415='',0.0,cast (kpi_0000000415 as double))+if( kpi_0000000416='',0.0,cast (kpi_0000000416 as double))+if( kpi_0000000417='',0.0,cast (kpi_0000000417 as double))+if( kpi_0000000418='',0.0,cast (kpi_0000000418 as double))+if( kpi_0000000419='',0.0,cast (kpi_0000000419 as double))+if( kpi_0000000420='',0.0,cast (kpi_0000000420 as double))+if( kpi_0000000421='',0.0,cast (kpi_0000000421 as double))+if( kpi_0000000422='',0.0,cast (kpi_0000000422 as double))+if( kpi_0000000423='',0.0,cast (kpi_0000000423 as double))+if( kpi_0000000424='',0.0,cast (kpi_0000000424 as double))+if( kpi_0000000425='',0.0,cast (kpi_0000000425 as double))+if( kpi_0000000426='',0.0,cast (kpi_0000000426 as double))+if( kpi_0000000427='',0.0,cast (kpi_0000000427 as double))+if( kpi_0000000428='',0.0,cast (kpi_0000000428 as double))+if( kpi_0000000429='',0.0,cast (kpi_0000000429 as double))+if( kpi_0000000430='',0.0,cast (kpi_0000000430 as double))+if( kpi_0000000431='',0.0,cast (kpi_0000000431 as double))+if( kpi_0000000432='',0.0,cast (kpi_0000000432 as double))+if( kpi_0000000433='',0.0,cast (kpi_0000000433 as double))+if( kpi_0000000434='',0.0,cast (kpi_0000000434 as double))+if( kpi_0000000435='',0.0,cast (kpi_0000000435 as double))+if( kpi_0000000436='',0.0,cast (kpi_0000000436 as double))+if( kpi_0000000437='',0.0,cast (kpi_0000000437 as double))+if( kpi_0000000438='',0.0,cast (kpi_0000000438 as double))+if( kpi_0000000439='',0.0,cast (kpi_0000000439 as double))+if( kpi_0000000440='',0.0,cast (kpi_0000000440 as double))+if( kpi_0000000441='',0.0,cast (kpi_0000000441 as double))+if( kpi_0000000442='',0.0,cast (kpi_0000000442 as double)))=0 then -1 else  (if( kpi_0000000422='',0.0,cast (kpi_0000000422 as double))+if( kpi_0000000423='',0.0,cast (kpi_0000000423 as double))+if( kpi_0000000424='',0.0,cast (kpi_0000000424 as double))+if( kpi_0000000425='',0.0,cast (kpi_0000000425 as double))+if( kpi_0000000426='',0.0,cast (kpi_0000000426 as double))+if( kpi_0000000427='',0.0,cast (kpi_0000000427 as double))+if( kpi_0000000428='',0.0,cast (kpi_0000000428 as double))+if( kpi_0000000429='',0.0,cast (kpi_0000000429 as double))+if( kpi_0000000430='',0.0,cast (kpi_0000000430 as double))+if( kpi_0000000431='',0.0,cast (kpi_0000000431 as double))+if( kpi_0000000432='',0.0,cast (kpi_0000000432 as double))+if( kpi_0000000433='',0.0,cast (kpi_0000000433 as double))+if( kpi_0000000434='',0.0,cast (kpi_0000000434 as double))+if( kpi_0000000435='',0.0,cast (kpi_0000000435 as double))+if( kpi_0000000436='',0.0,cast (kpi_0000000436 as double))+if( kpi_0000000437='',0.0,cast (kpi_0000000437 as double))+if( kpi_0000000438='',0.0,cast (kpi_0000000438 as double))+if( kpi_0000000439='',0.0,cast (kpi_0000000439 as double))+if( kpi_0000000440='',0.0,cast (kpi_0000000440 as double))+if( kpi_0000000441='',0.0,cast (kpi_0000000441 as double))+if( kpi_0000000442='',0.0,cast (kpi_0000000442 as double)))/(if( kpi_0000000411='',0.0,cast (kpi_0000000411 as double))+if( kpi_0000000412='',0.0,cast (kpi_0000000412 as double))+if( kpi_0000000413='',0.0,cast (kpi_0000000413 as double))+if( kpi_0000000414='',0.0,cast (kpi_0000000414 as double))+if( kpi_0000000415='',0.0,cast (kpi_0000000415 as double))+if( kpi_0000000416='',0.0,cast (kpi_0000000416 as double))+if( kpi_0000000417='',0.0,cast (kpi_0000000417 as double))+if( kpi_0000000418='',0.0,cast (kpi_0000000418 as double))+if( kpi_0000000419='',0.0,cast (kpi_0000000419 as double))+if( kpi_0000000420='',0.0,cast (kpi_0000000420 as double))+if( kpi_0000000421='',0.0,cast (kpi_0000000421 as double))+if( kpi_0000000422='',0.0,cast (kpi_0000000422 as double))+if( kpi_0000000423='',0.0,cast (kpi_0000000423 as double))+if( kpi_0000000424='',0.0,cast (kpi_0000000424 as double))+if( kpi_0000000425='',0.0,cast (kpi_0000000425 as double))+if( kpi_0000000426='',0.0,cast (kpi_0000000426 as double))+if( kpi_0000000427='',0.0,cast (kpi_0000000427 as double))+if( kpi_0000000428='',0.0,cast (kpi_0000000428 as double))+if( kpi_0000000429='',0.0,cast (kpi_0000000429 as double))+if( kpi_0000000430='',0.0,cast (kpi_0000000430 as double))+if( kpi_0000000431='',0.0,cast (kpi_0000000431 as double))+if( kpi_0000000432='',0.0,cast (kpi_0000000432 as double))+if( kpi_0000000433='',0.0,cast (kpi_0000000433 as double))+if( kpi_0000000434='',0.0,cast (kpi_0000000434 as double))+if( kpi_0000000435='',0.0,cast (kpi_0000000435 as double))+if( kpi_0000000436='',0.0,cast (kpi_0000000436 as double))+if( kpi_0000000437='',0.0,cast (kpi_0000000437 as double))+if( kpi_0000000438='',0.0,cast (kpi_0000000438 as double))+if( kpi_0000000439='',0.0,cast (kpi_0000000439 as double))+if( kpi_0000000440='',0.0,cast (kpi_0000000440 as double))+if( kpi_0000000441='',0.0,cast (kpi_0000000441 as double))+if( kpi_0000000442='',0.0,cast (kpi_0000000442 as double))) end) as pusch_mcsge11_ratio,
            (case when (if( kpi_0000000443='',0.0,cast (kpi_0000000443 as double))+if( kpi_0000000444='',0.0,cast (kpi_0000000444 as double))+if( kpi_0000000445='',0.0,cast (kpi_0000000445 as double))+if( kpi_0000000446='',0.0,cast (kpi_0000000446 as double))+if( kpi_0000000447='',0.0,cast (kpi_0000000447 as double))+if( kpi_0000000448='',0.0,cast (kpi_0000000448 as double))+if( kpi_0000000449='',0.0,cast (kpi_0000000449 as double))+if( kpi_0000000450='',0.0,cast (kpi_0000000450 as double))+if( kpi_0000000451='',0.0,cast (kpi_0000000451 as double))+if( kpi_0000000452='',0.0,cast (kpi_0000000452 as double))+if( kpi_0000000453='',0.0,cast (kpi_0000000453 as double))+if( kpi_0000000454='',0.0,cast (kpi_0000000454 as double))+if( kpi_0000000455='',0.0,cast (kpi_0000000455 as double))+if( kpi_0000000456='',0.0,cast (kpi_0000000456 as double))+if( kpi_0000000457='',0.0,cast (kpi_0000000457 as double))+if( kpi_0000000458='',0.0,cast (kpi_0000000458 as double))+if( kpi_0000000459='',0.0,cast (kpi_0000000459 as double))+if( kpi_0000000460='',0.0,cast (kpi_0000000460 as double))+if( kpi_0000000461='',0.0,cast (kpi_0000000461 as double))+if( kpi_0000000462='',0.0,cast (kpi_0000000462 as double))+if( kpi_0000000463='',0.0,cast (kpi_0000000463 as double))+if( kpi_0000000464='',0.0,cast (kpi_0000000464 as double))+if( kpi_0000000465='',0.0,cast (kpi_0000000465 as double))+if( kpi_0000000466='',0.0,cast (kpi_0000000466 as double))+if( kpi_0000000467='',0.0,cast (kpi_0000000467 as double))+if( kpi_0000000468='',0.0,cast (kpi_0000000468 as double))+if( kpi_0000000469='',0.0,cast (kpi_0000000469 as double))+if( kpi_0000000470='',0.0,cast (kpi_0000000470 as double))+if( kpi_0000000471='',0.0,cast (kpi_0000000471 as double))+if( kpi_0000000472='',0.0,cast (kpi_0000000472 as double))+if( kpi_0000000473='',0.0,cast (kpi_0000000473 as double))+if( kpi_0000000474='',0.0,cast (kpi_0000000474 as double)))=0 then -1 else  (if( kpi_0000000454='',0.0,cast (kpi_0000000454 as double))+if( kpi_0000000455='',0.0,cast (kpi_0000000455 as double))+if( kpi_0000000456='',0.0,cast (kpi_0000000456 as double))+if( kpi_0000000457='',0.0,cast (kpi_0000000457 as double))+if( kpi_0000000458='',0.0,cast (kpi_0000000458 as double))+if( kpi_0000000459='',0.0,cast (kpi_0000000459 as double))+if( kpi_0000000460='',0.0,cast (kpi_0000000460 as double))+if( kpi_0000000461='',0.0,cast (kpi_0000000461 as double))+if( kpi_0000000462='',0.0,cast (kpi_0000000462 as double))+if( kpi_0000000463='',0.0,cast (kpi_0000000463 as double))+if( kpi_0000000464='',0.0,cast (kpi_0000000464 as double))+if( kpi_0000000465='',0.0,cast (kpi_0000000465 as double))+if( kpi_0000000466='',0.0,cast (kpi_0000000466 as double))+if( kpi_0000000467='',0.0,cast (kpi_0000000467 as double))+if( kpi_0000000468='',0.0,cast (kpi_0000000468 as double))+if( kpi_0000000469='',0.0,cast (kpi_0000000469 as double))+if( kpi_0000000470='',0.0,cast (kpi_0000000470 as double))+if( kpi_0000000471='',0.0,cast (kpi_0000000471 as double))+if( kpi_0000000472='',0.0,cast (kpi_0000000472 as double))+if( kpi_0000000473='',0.0,cast (kpi_0000000473 as double))+if( kpi_0000000474='',0.0,cast (kpi_0000000474 as double)))/(if( kpi_0000000443='',0.0,cast (kpi_0000000443 as double))+if( kpi_0000000444='',0.0,cast (kpi_0000000444 as double))+if( kpi_0000000445='',0.0,cast (kpi_0000000445 as double))+if( kpi_0000000446='',0.0,cast (kpi_0000000446 as double))+if( kpi_0000000447='',0.0,cast (kpi_0000000447 as double))+if( kpi_0000000448='',0.0,cast (kpi_0000000448 as double))+if( kpi_0000000449='',0.0,cast (kpi_0000000449 as double))+if( kpi_0000000450='',0.0,cast (kpi_0000000450 as double))+if( kpi_0000000451='',0.0,cast (kpi_0000000451 as double))+if( kpi_0000000452='',0.0,cast (kpi_0000000452 as double))+if( kpi_0000000453='',0.0,cast (kpi_0000000453 as double))+if( kpi_0000000454='',0.0,cast (kpi_0000000454 as double))+if( kpi_0000000455='',0.0,cast (kpi_0000000455 as double))+if( kpi_0000000456='',0.0,cast (kpi_0000000456 as double))+if( kpi_0000000457='',0.0,cast (kpi_0000000457 as double))+if( kpi_0000000458='',0.0,cast (kpi_0000000458 as double))+if( kpi_0000000459='',0.0,cast (kpi_0000000459 as double))+if( kpi_0000000460='',0.0,cast (kpi_0000000460 as double))+if( kpi_0000000461='',0.0,cast (kpi_0000000461 as double))+if( kpi_0000000462='',0.0,cast (kpi_0000000462 as double))+if( kpi_0000000463='',0.0,cast (kpi_0000000463 as double))+if( kpi_0000000464='',0.0,cast (kpi_0000000464 as double))+if( kpi_0000000465='',0.0,cast (kpi_0000000465 as double))+if( kpi_0000000466='',0.0,cast (kpi_0000000466 as double))+if( kpi_0000000467='',0.0,cast (kpi_0000000467 as double))+if( kpi_0000000468='',0.0,cast (kpi_0000000468 as double))+if( kpi_0000000469='',0.0,cast (kpi_0000000469 as double))+if( kpi_0000000470='',0.0,cast (kpi_0000000470 as double))+if( kpi_0000000471='',0.0,cast (kpi_0000000471 as double))+if( kpi_0000000472='',0.0,cast (kpi_0000000472 as double))+if( kpi_0000000473='',0.0,cast (kpi_0000000473 as double))+if( kpi_0000000474='',0.0,cast (kpi_0000000474 as double))) end) as pdsch_mcsge11_ratio,
            (case when (if( kpi_0000000636='',0.0,cast (kpi_0000000636 as double))+if( kpi_0000000637='',0.0,cast (kpi_0000000637 as double))+if( kpi_0000000638='',0.0,cast (kpi_0000000638 as double))+if( kpi_0000000639='',0.0,cast (kpi_0000000639 as double))+if( kpi_0000000640='',0.0,cast (kpi_0000000640 as double))+if( kpi_0000000641='',0.0,cast (kpi_0000000641 as double))+if( kpi_0000000642='',0.0,cast (kpi_0000000642 as double))+if( kpi_0000000643='',0.0,cast (kpi_0000000643 as double))+if( kpi_0000000644='',0.0,cast (kpi_0000000644 as double))+if( kpi_0000000645='',0.0,cast (kpi_0000000645 as double))+if( kpi_0000000646='',0.0,cast (kpi_0000000646 as double))+if( kpi_0000000647='',0.0,cast (kpi_0000000647 as double))+if( kpi_0000000648='',0.0,cast (kpi_0000000648 as double))+if( kpi_0000000649='',0.0,cast (kpi_0000000649 as double))+if( kpi_0000000650='',0.0,cast (kpi_0000000650 as double))+if( kpi_0000000651='',0.0,cast (kpi_0000000651 as double)))=0 then -1 else (if( kpi_0000000643='',0.0,cast (kpi_0000000643 as double))+if( kpi_0000000644='',0.0,cast (kpi_0000000644 as double))+if( kpi_0000000645='',0.0,cast (kpi_0000000645 as double))+if( kpi_0000000646='',0.0,cast (kpi_0000000646 as double))+if( kpi_0000000647='',0.0,cast (kpi_0000000647 as double))+if( kpi_0000000648='',0.0,cast (kpi_0000000648 as double))+if( kpi_0000000649='',0.0,cast (kpi_0000000649 as double))+if( kpi_0000000650='',0.0,cast (kpi_0000000650 as double))+if( kpi_0000000651='',0.0,cast (kpi_0000000651 as double)))/(if( kpi_0000000636='',0.0,cast (kpi_0000000636 as double))+if( kpi_0000000637='',0.0,cast (kpi_0000000637 as double))+if( kpi_0000000638='',0.0,cast (kpi_0000000638 as double))+if( kpi_0000000639='',0.0,cast (kpi_0000000639 as double))+if( kpi_0000000640='',0.0,cast (kpi_0000000640 as double))+if( kpi_0000000641='',0.0,cast (kpi_0000000641 as double))+if( kpi_0000000642='',0.0,cast (kpi_0000000642 as double))+if( kpi_0000000643='',0.0,cast (kpi_0000000643 as double))+if( kpi_0000000644='',0.0,cast (kpi_0000000644 as double))+if( kpi_0000000645='',0.0,cast (kpi_0000000645 as double))+if( kpi_0000000646='',0.0,cast (kpi_0000000646 as double))+if( kpi_0000000647='',0.0,cast (kpi_0000000647 as double))+if( kpi_0000000648='',0.0,cast (kpi_0000000648 as double))+if( kpi_0000000649='',0.0,cast (kpi_0000000649 as double))+if( kpi_0000000650='',0.0,cast (kpi_0000000650 as double))+if( kpi_0000000651='',0.0,cast (kpi_0000000651 as double))) end) as  cqig7_ratio,
            coalesce(cast (if(kpi_0000000422='' or kpi_0000000422='NULL','-1',kpi_0000000422) as double),-1) as kpi_0000000422,
            coalesce(cast (if(kpi_0000000423='' or kpi_0000000423='NULL','-1',kpi_0000000423) as double),-1) as kpi_0000000423,
            coalesce(cast (if(kpi_0000000424='' or kpi_0000000424='NULL','-1',kpi_0000000424) as double),-1) as kpi_0000000424,
            coalesce(cast (if(kpi_0000000425='' or kpi_0000000425='NULL','-1',kpi_0000000425) as double),-1) as kpi_0000000425,
            coalesce(cast (if(kpi_0000000426='' or kpi_0000000426='NULL','-1',kpi_0000000426) as double),-1) as kpi_0000000426,
            coalesce(cast (if(kpi_0000000427='' or kpi_0000000427='NULL','-1',kpi_0000000427) as double),-1) as kpi_0000000427,
            coalesce(cast (if(kpi_0000000428='' or kpi_0000000428='NULL','-1',kpi_0000000428) as double),-1) as kpi_0000000428,
            coalesce(cast (if(kpi_0000000429='' or kpi_0000000429='NULL','-1',kpi_0000000429) as double),-1) as kpi_0000000429,
            coalesce(cast (if(kpi_0000000430='' or kpi_0000000430='NULL','-1',kpi_0000000430) as double),-1) as kpi_0000000430,
            coalesce(cast (if(kpi_0000000431='' or kpi_0000000431='NULL','-1',kpi_0000000431) as double),-1) as kpi_0000000431,
            coalesce(cast (if(kpi_0000000432='' or kpi_0000000432='NULL','-1',kpi_0000000432) as double),-1) as kpi_0000000432,
            coalesce(cast (if(kpi_0000000433='' or kpi_0000000433='NULL','-1',kpi_0000000433) as double),-1) as kpi_0000000433,
            coalesce(cast (if(kpi_0000000434='' or kpi_0000000434='NULL','-1',kpi_0000000434) as double),-1) as kpi_0000000434,
            coalesce(cast (if(kpi_0000000435='' or kpi_0000000435='NULL','-1',kpi_0000000435) as double),-1) as kpi_0000000435,
            coalesce(cast (if(kpi_0000000436='' or kpi_0000000436='NULL','-1',kpi_0000000436) as double),-1) as kpi_0000000436,
            coalesce(cast (if(kpi_0000000437='' or kpi_0000000437='NULL','-1',kpi_0000000437) as double),-1) as kpi_0000000437,
            coalesce(cast (if(kpi_0000000438='' or kpi_0000000438='NULL','-1',kpi_0000000438) as double),-1) as kpi_0000000438,
            coalesce(cast (if(kpi_0000000439='' or kpi_0000000439='NULL','-1',kpi_0000000439) as double),-1) as kpi_0000000439,
            coalesce(cast (if(kpi_0000000440='' or kpi_0000000440='NULL','-1',kpi_0000000440) as double),-1) as kpi_0000000440,
            coalesce(cast (if(kpi_0000000441='' or kpi_0000000441='NULL','-1',kpi_0000000441) as double),-1) as kpi_0000000441,
            coalesce(cast (if(kpi_0000000442='' or kpi_0000000442='NULL','-1',kpi_0000000442) as double),-1) as kpi_0000000442,
            coalesce(cast (if(kpi_0000000454='' or kpi_0000000454='NULL','-1',kpi_0000000454) as double),-1) as kpi_0000000454,
            coalesce(cast (if(kpi_0000000455='' or kpi_0000000455='NULL','-1',kpi_0000000455) as double),-1) as kpi_0000000455,
            coalesce(cast (if(kpi_0000000456='' or kpi_0000000456='NULL','-1',kpi_0000000456) as double),-1) as kpi_0000000456,
            coalesce(cast (if(kpi_0000000457='' or kpi_0000000457='NULL','-1',kpi_0000000457) as double),-1) as kpi_0000000457,
            coalesce(cast (if(kpi_0000000458='' or kpi_0000000458='NULL','-1',kpi_0000000458) as double),-1) as kpi_0000000458,
            coalesce(cast (if(kpi_0000000459='' or kpi_0000000459='NULL','-1',kpi_0000000459) as double),-1) as kpi_0000000459,
            coalesce(cast (if(kpi_0000000460='' or kpi_0000000460='NULL','-1',kpi_0000000460) as double),-1) as kpi_0000000460,
            coalesce(cast (if(kpi_0000000461='' or kpi_0000000461='NULL','-1',kpi_0000000461) as double),-1) as kpi_0000000461,
            coalesce(cast (if(kpi_0000000462='' or kpi_0000000462='NULL','-1',kpi_0000000462) as double),-1) as kpi_0000000462,
            coalesce(cast (if(kpi_0000000463='' or kpi_0000000463='NULL','-1',kpi_0000000463) as double),-1) as kpi_0000000463,
            coalesce(cast (if(kpi_0000000464='' or kpi_0000000464='NULL','-1',kpi_0000000464) as double),-1) as kpi_0000000464,
            coalesce(cast (if(kpi_0000000465='' or kpi_0000000465='NULL','-1',kpi_0000000465) as double),-1) as kpi_0000000465,
            coalesce(cast (if(kpi_0000000466='' or kpi_0000000466='NULL','-1',kpi_0000000466) as double),-1) as kpi_0000000466,
            coalesce(cast (if(kpi_0000000467='' or kpi_0000000467='NULL','-1',kpi_0000000467) as double),-1) as kpi_0000000467,
            coalesce(cast (if(kpi_0000000468='' or kpi_0000000468='NULL','-1',kpi_0000000468) as double),-1) as kpi_0000000468,
            coalesce(cast (if(kpi_0000000469='' or kpi_0000000469='NULL','-1',kpi_0000000469) as double),-1) as kpi_0000000469,
            coalesce(cast (if(kpi_0000000470='' or kpi_0000000470='NULL','-1',kpi_0000000470) as double),-1) as kpi_0000000470,
            coalesce(cast (if(kpi_0000000471='' or kpi_0000000471='NULL','-1',kpi_0000000471) as double),-1) as kpi_0000000471,
            coalesce(cast (if(kpi_0000000472='' or kpi_0000000472='NULL','-1',kpi_0000000472) as double),-1) as kpi_0000000472,
            coalesce(cast (if(kpi_0000000473='' or kpi_0000000473='NULL','-1',kpi_0000000473) as double),-1) as kpi_0000000473,
            coalesce(cast (if(kpi_0000000474='' or kpi_0000000474='NULL','-1',kpi_0000000474) as double),-1) as kpi_0000000474,
            coalesce(cast (if(kpi_0000000643='' or kpi_0000000643='NULL','-1',kpi_0000000643) as double),-1) as kpi_0000000643,
            coalesce(cast (if(kpi_0000000644='' or kpi_0000000644='NULL','-1',kpi_0000000644) as double),-1) as kpi_0000000644,
            coalesce(cast (if(kpi_0000000645='' or kpi_0000000645='NULL','-1',kpi_0000000645) as double),-1) as kpi_0000000645,
            coalesce(cast (if(kpi_0000000646='' or kpi_0000000646='NULL','-1',kpi_0000000646) as double),-1) as kpi_0000000646,
            coalesce(cast (if(kpi_0000000647='' or kpi_0000000647='NULL','-1',kpi_0000000647) as double),-1) as kpi_0000000647,
            coalesce(cast (if(kpi_0000000648='' or kpi_0000000648='NULL','-1',kpi_0000000648) as double),-1) as kpi_0000000648,
            coalesce(cast (if(kpi_0000000649='' or kpi_0000000649='NULL','-1',kpi_0000000649) as double),-1) as kpi_0000000649,
            coalesce(cast (if(kpi_0000000650='' or kpi_0000000650='NULL','-1',kpi_0000000650) as double),-1) as kpi_0000000650,
            coalesce(cast (if(kpi_0000000651='' or kpi_0000000651='NULL','-1',kpi_0000000651) as double),-1) as kpi_0000000651,
            coalesce(cast(format_datetime(cast(start_time as timestamp),'yyyyMMdd') as int),-1) as daytime,
            coalesce(cast(format_datetime(cast(start_time as timestamp),'yyyyMMddHH') as int),-1) as hourtime
            from  hive.cnio.pm_4g where pm_date ='%s'  and  substr(start_time,12,2)= '%s' 
         ''' % (unload_hour_list[i][0], unload_hour_list[i][1])
        data = get_pm_data(sql_hour)
        if len(data):
            siteinfo_data = get_siteinfo()
            data = pd.merge(data, siteinfo_data, on=["oid", "cellid"], how='left')
            postgre_expert(data)
            print("pm小时粒度数据同步成功")
        else:
            print("pm小时粒度数据同步失败")

if __name__ == '__main__':
    days_num=${delay_days}
    date_list, time_list = get_time_list(days_num)

    load_data_hour = upload_hour_list(date_list)
    unload_hour_list = unload_hour(time_list, load_data_hour)
    print("未同步pm数据时间清单 {0}".format(unload_hour_list))
    if len(unload_hour_list):
        data_process(unload_hour_list)
        print("pm小时粒度数据完成同步")

    else:
        print("未同步数据清单为空")
