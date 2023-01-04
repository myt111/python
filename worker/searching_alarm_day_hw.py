import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import datetime as dt
from io import StringIO


def get_date():
    today = dt.datetime.today()
    yestoday = today + dt.timedelta(days=(-1))

    yestoday_time = yestoday.strftime('%Y-%m-%d')
    yestoday_time_extra = yestoday.strftime('%Y%m%d')
    return yestoday_time_extra, yestoday_time

def postgre_execute_hw(yestoday):
    conn = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502",
                            host="10.1.77.51", port="5432")
    print("Trace Postgres Connection success")

    cur = conn.cursor()
    # 获取taskid
    sql_alarm = """
    select
            alarm_name ,
            alarm_level,
            to_char(cast(start_time as timestamp) , 'yyyy-mm-dd') as start_time ,
            cast(enodebid as int) as enodebid,
            locate_info
        from
            cnio.alarm_hw_hour ahh
        where
            enodebid != '		-'
            and to_char(cast(start_time as timestamp) , 'yyyy-mm-dd')= '%s'

    """ % yestoday
    cur.execute(sql_alarm)
    alarm_enb = cur.fetchall()
    alarm_enb_pd = pd.DataFrame(alarm_enb)
    des = cur.description
    colname = []
    for item in des:
        colname.append(item[0])
    alarm_enb_pd.columns = colname

    sql_siteinfo = """
    select siteid,cellid,ci from cnio.siteinfo s where rattype ='4G' and data_date = (select max(data_date) from cnio.siteinfo s2)
    """
    cur.execute(sql_siteinfo)
    siteinfo = cur.fetchall()
    siteinfo_pd = pd.DataFrame(siteinfo)
    des = cur.description
    colname = []
    for item in des:
        colname.append(item[0])
    siteinfo_pd.columns = colname

    cur.close()
    conn.close()
    print("alarm Postgres Connection close")
    return alarm_enb_pd, siteinfo_pd

def postgre_execute_zx(yestoday):
    conn = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502",
                            host="10.1.77.51", port="5432")
    print("Trace Postgres Connection success")

    cur = conn.cursor()
    # 获取taskid
    sql_alarm = """
    select
            alarm_name ,
            to_char(cast(start_time as timestamp) , 'yyyy-mm-dd') as start_time ,
            cast(enodebid as int) as enodebid,
            addition_text
        from
            "expert-system".cnio.alarm_zx_hour
        where
            enodebid != '		-' and start_time !='发生时间'
            and to_char(cast(start_time as timestamp) , 'yyyy-mm-dd')= '%s' 
    """ % yestoday
    cur.execute(sql_alarm)
    alarm_enb = cur.fetchall()
    alarm_enb_pd = pd.DataFrame(alarm_enb)
    des = cur.description
    colname = []
    for item in des:
        colname.append(item[0])
    alarm_enb_pd.columns = colname

    sql_siteinfo = """
    select siteid,cellid,ci from cnio.siteinfo s where rattype ='4G' and data_date = (select max(data_date) from cnio.siteinfo s2)
    """
    cur.execute(sql_siteinfo)
    siteinfo = cur.fetchall()
    siteinfo_pd = pd.DataFrame(siteinfo)
    des = cur.description
    colname = []
    for item in des:
        colname.append(item[0])
    siteinfo_pd.columns = colname

    cur.close()
    conn.close()
    print("alarm Postgres Connection close")
    return alarm_enb_pd, siteinfo_pd


def postgre_upload(data):
    #engine = create_engine('postgresql://expert-system:YJY_exp#exp502@10.1.77.51:5432/expert-system')
    #data.to_sql("alarm_hz_day", engine, schema="searching", index=False, if_exists='append', method='multi')
    output = StringIO()
    data.to_csv(output, sep='\t', index=False, header=False)
    output1 = output.getvalue()
    conn = psycopg2.connect(host="10.1.77.51", user="expert-system", password="YJY_exp#exp502",
                            database="expert-system", port="5432",options="-c search_path=searching")
    #options="-c search_path=cnio"
    cur = conn.cursor()
    cur.copy_from(StringIO(output1), 'alarm_hz_day',null='')
    conn.commit()
    cur.close()
    conn.close()
    print("alarm_hz_day sync: success")

def postgre_alarm_hw_day(alarm, siteinfo):
    if len(alarm) != 0 and len(siteinfo) != 0:
        alarm["flag"] = alarm["locate_info"].apply(lambda x: 1 if "小区标识" in x else 0)
        alarm_enb = alarm[alarm["flag"] == 0]
        alarm_cell = alarm[alarm["flag"] == 1]

        alarm_enb_cell = pd.merge(alarm_enb, siteinfo, left_on='enodebid', right_on='siteid', how='inner')

        alarm_enb_cell = alarm_enb_cell[["start_time", "alarm_name", "alarm_level", "enodebid", "cellid", "ci"]]

        # shape[0] 表示有多少行，shape[1]表示有多少列
        alarm_cell.index = range(alarm_cell.shape[0])
        for i in alarm_cell.index:
            # loc[0] 按行进行获取，切片,loc[i,"locate_info"]根据i进行切分，获取指定的列
            temp = alarm_cell.loc[i, "locate_info"]
            temp_list = temp.split(',')
            for j in temp_list:
                if '小区标识' in j and '本地小区标识' not in j:
                    cellid = j.split("=")
                    alarm_cell.loc[i, "cellid"] = int(cellid[1])
        alarm_cell = alarm_cell[["start_time", "alarm_name", "alarm_level", "enodebid", "cellid"]]
        alarm_cell["eci"] = alarm_cell[["enodebid", "cellid"]].apply(lambda x: float(x["enodebid"] * 256 + x["cellid"]),
                                                                     axis=1)
        alarm_enb_cell.rename(columns={"ci": "eci"}, inplace=True)

        alarm_data = pd.concat([alarm_enb_cell, alarm_cell])

        alarm_data["vendor"] = "华为"

        ### 计算指标 pandas  dataframe
        alarm_all_count = alarm_data.groupby(["start_time", "enodebid", "cellid", "eci", "vendor"]).count()
        alarm_all_count = alarm_all_count.reset_index()
        alarm_all_count.rename(columns={"alarm_name": "total_num"}, inplace=True)
        alarm_all_count = alarm_all_count[["start_time", "enodebid", "cellid", "eci", "vendor", "total_num"]]

        alarm_type_count = alarm_data.groupby(["eci", "alarm_level"]).count()
        alarm_type_count = alarm_type_count.reset_index()
        alarm_type_count.rename(columns={"start_time": "alarm_num"}, inplace=True)
        alarm_type_count = alarm_type_count[["eci", "alarm_level", "alarm_num"]]

        ## 重要告警
        alarm_importance = alarm_type_count[alarm_type_count["alarm_level"] == "重要"]
        alarm_importance.rename(columns={"alarm_num": "important_alarm_num"}, inplace=True)
        ## 紧急告警
        alarm_urgent = alarm_type_count[alarm_type_count["alarm_level"] == "紧急"]
        alarm_urgent.rename(columns={"alarm_num": "urgent_alarm_num"}, inplace=True)
        ## 次要告警
        alarm_less_importance = alarm_type_count[alarm_type_count["alarm_level"] == "次要"]
        alarm_less_importance.rename(columns={"alarm_num": "less_important_alarm_num"}, inplace=True)
        ## 提示告警
        alarm_hint = alarm_type_count[alarm_type_count["alarm_level"] == "提示"]
        alarm_hint.rename(columns={"alarm_num": "hint_alarm_num"}, inplace=True)

        ## 告警关联到主表上
        alarm_hw = pd.merge(alarm_all_count, alarm_importance, left_on=["eci"], right_on="eci", how='left')
        alarm_hw = pd.merge(alarm_hw, alarm_urgent, left_on=["eci"], right_on="eci", how='left')
        alarm_hw = pd.merge(alarm_hw, alarm_less_importance, left_on=["eci"], right_on="eci", how='left')
        alarm_hw = pd.merge(alarm_hw, alarm_hint, left_on=["eci"], right_on="eci", how='left')

        alarm_name_num = alarm_data.groupby(["eci", 'alarm_name'])["start_time"].count()
        alarm_name_num = alarm_name_num.reset_index()
        alarm_name_num.rename(columns={"start_time": "alarm_num"}, inplace=True)
        alarm_name_num["sort_id"] = alarm_name_num["alarm_num"].groupby(alarm_name_num["eci"]).rank(ascending=0)

        alarm_top1 = alarm_name_num[alarm_name_num["sort_id"] == 1]
        alarm_top1.rename(columns={"alarm_name": "top1_alarm_name", "sort_id": "top1_alarm_num"}, inplace=True)

        alarm_top2 = alarm_name_num[alarm_name_num["sort_id"] == 2]
        alarm_top2.rename(columns={"alarm_name": "top2_alarm_name", "sort_id": "top2_alarm_num"}, inplace=True)

        alarm_top3 = alarm_name_num[alarm_name_num["sort_id"] == 3]
        alarm_top3.rename(columns={"alarm_name": "top3_alarm_name", "sort_id": "top3_alarm_num"}, inplace=True)

        alarm_hw = pd.merge(alarm_hw, alarm_top1, left_on=["eci"], right_on="eci", how='left')
        alarm_hw = pd.merge(alarm_hw, alarm_top2, left_on=["eci"], right_on="eci", how='left')
        alarm_hw = pd.merge(alarm_hw, alarm_top3, left_on=["eci"], right_on="eci", how='left')

        alarm_hw = alarm_hw[["start_time", "enodebid", "cellid",
                             "eci", "vendor", "total_num", "important_alarm_num",
                             "urgent_alarm_num", "less_important_alarm_num", "hint_alarm_num",
                             "top1_alarm_name", "top1_alarm_num",
                             "top2_alarm_name", "top2_alarm_num",
                             "top3_alarm_name", "top3_alarm_num"]]
        alarm_hw.rename(columns={"start_time": "data_time", "enodebid": "siteid"}, inplace=True)

      #  alarm_hw = alarm_hw.fillna('0')

        alarm_hw["top1_alarm_name"] = alarm_hw["top1_alarm_name"].astype(str)

        return alarm_hw

if __name__ == '__main__':
    # yestoday_time_extra, yestoday = get_date()
    yestoday = '2022-09-12'

    alarm, siteinfo = postgre_execute_hw(yestoday)
    alarm_hw =  postgre_alarm_hw_day(alarm, siteinfo)
    print("ok")


    postgre_upload(alarm_hw)

