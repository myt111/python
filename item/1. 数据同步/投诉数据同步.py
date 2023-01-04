import pandas
import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import datetime as dt
import zipfile
from io import StringIO


def data_upload(data, table_name, schema_name):
    #engine = create_engine('postgresql://expert-system:YJY_exp#exp502@10.1.77.51:5432/expert-system')
    engine = create_engine('postgresql://expert-system:YJY_exp#exp502@133.160.191.111:5432/expert-system')
    data.to_sql(table_name, engine, schema=schema_name, index=False, if_exists='append')


def data_load(data, table, expert_db_account):
    data.to_csv("test.csv", index=False)
    output = StringIO()
    data.to_csv(output, sep='\t', index=False, header=False)
    output1 = output.getvalue()
    conn = psycopg2.connect(host=expert_db_account["host"], user=expert_db_account["user"],
                            password=expert_db_account["password"], database=expert_db_account["database"],
                            port=expert_db_account["port"], options=expert_db_account["options"])
    cur = conn.cursor()
    cur.copy_from(StringIO(output1), table, sep='\t', null='')
    conn.commit()
    cur.close()
    conn.close()
    print("异常呼叫话单同步成功")


def get_month():
    today = dt.datetime.today()
    last_month_last_day = dt.datetime(today.year, today.month, 1) - dt.timedelta(days=1)
    last_month = last_month_last_day.strftime('%Y%m')
    return last_month




if __name__ == "__main__":
    # expert_db_account = {"database": "expert-system", "user": "expert-system", "password": "YJY_exp#exp502",
    #                      "host": "10.1.77.51", "port": "5432", "options": "-c search_path=cnio"}
    root = r"/bigdata/data_warehouse/zhengzhou/7_COMPLAINT/"
    #root = "D:\\360安全浏览器下载\\"
    last_month = get_month()
    #last_month='202209'
    path = root + last_month + "/"
    if os.path.exists(path):
        for root, dirs, files in os.walk(path):
            for file in files:
                if last_month in file:
                    content = pd.read_excel(path + file)
                    if len(content) != 0:
                        content.columns = ["complaint_time", "serial_number", "phone_number", "work_order_type",
                                           "channel",
                                           "risk_type", "return_order", "handle", "content", "other_needs",
                                           "user_manner", "star_rating", "acceptance_time", "complaint_address",
                                           "network",
                                           "service_type", "building_number", "scene", "cause", "siteid", "alarm",
                                           "cause_detail", "county", "management_grid", "gaode_lon", "gaode_lat", "lon",
                                           "lat", "recorder", "personnel_account"]
                        try:
                            data_upload(content,"dwd_complaint_r2","cnio")
                            print("数据导入dwd_complaint_r2表单成功")
                        except Exception as e:
                            print("数据入库异常，异常：{0}".format(e))
                        ## 入库
                        content_sub = content[["complaint_time", "work_order_type", "channel", "serial_number",
                                               "handle", "content", "cause", "cause_detail", "county",
                                               "management_grid",
                                               "siteid", "alarm", "lon", "lat", "building_number", "network",
                                               "service_type", "scene"]]
                        content_sub["complaint_date"] = content_sub[["complaint_time"]].apply(
                            lambda x: dt.datetime.strftime(
                                dt.datetime.strptime(x["complaint_time"], "%Y-%m-%d %H:%M:%S"), "%Y-%m-%d"), axis=1)
                        content_sub["area"] = None
                        #content_sub = content_sub.loc[:,~content_sub.columns.isin(["complaint_time"])]
                        content_sub = content_sub[["complaint_date", "work_order_type", "channel", "serial_number",
                                                   "handle", "content", "cause", "cause_detail", "county",
                                                   "management_grid",
                                                   "siteid", "alarm", "lon", "lat", "area", "building_number",
                                                   "network",
                                                   "service_type", "scene"]]
                        try:
                            data_upload(content_sub,"dwd_complaint","cnio")
                            print("数据导入dwd_complaint表单成功")
                        except Exception as e:
                            print("数据入库异常，异常：{0}".format(e))
                        print("ok")

                    else:
                        print("数据为空，入库失败")
                else:
                    print("{0}文件夹中无数据文件".format(last_month))
    else:
        print("{0}文件夹路径不存在".format(last_month))
