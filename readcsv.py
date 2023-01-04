import pandas as pd
import trino
from trino import transaction
import psycopg2
from sqlalchemy import create_engine
from io import StringIO

import psycopg2
import csv

def postgre_create():
    database_postgres = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502",
                                     host="10.1.77.51", port="5432")
    path = r"C:\Users\Lenovo\Desktop\工作\需求分析\15. 小时天周字段调整\告警小时天周汇聚\华为中兴导入同一张表\alarm_4G_20220919\中兴4G历史告警0919.csv"
    data = pd.read_csv(path, encoding='gbk')

    cursor = database_postgres.cursor()
    print('Inserting data...')

    cursor.execute("DROP TABLE IF EXISTS cnio.alarm_zx_hour;")
# 自定义数据类型
    cursor.execute("create table cnio.alarm_zx_hour ("
                   "rootalarm_flag text NULL,"
                   "ack_status text NULL,"
                   "alarm_level text NULL,"
                   "network text NULL,"
                   "locate_info text NULL,"
                   "system_type text NULL,"
                   "alarm_name text NULL,"
                   "start_time text NULL,"
                   "network_type text NULL,"
                   "alarm_type text NULL,"
                   "alarm_reason text NULL,"
                   "addition_text text NULL,"
                   "admc text NULL,"
                   "clear_time text NULL,"
                   "count text NULL,"
                   "alarm_object_name text NULL,"
                   "alarm_object_type text NULL,"
                   "single_board text NULL,"
                   "enodebid text NULL,"
                   "alarm_source text NULL,"
                   "alarm_object_id text NULL,"
                   "alarm_object_dn text NULL,"
                   "alarm_id_standard text NULL,"
                   "network_type2 text NULL,"
                   "alarm_flag text NULL,"
                   "influence_network text NULL,"
                   "influence_network_locate text NULL,"
                   "revise_time text NULL,"
                   "addition text NULL,"
                   "confirm_object text NULL,"
                   "confirm_system text NULL,"
                   "confirm_time text NULL,"
                   "confirm_info text NULL,"
                   "clear_object text NULL,"
                   "clear_system text NULL,"
                   "recovery_method text NULL,"
                   "clear_info text NULL,"
                   "alarm_notes text NULL,"
                   "notes_object text NULL,"
                   "notes_system text NULL,"
                   "notes_time text NULL,"
                   "alarm_serial_number text NULL,"
                   "network_ip text NULL,"
                   "link text NULL,"
                   "network_group text NULL,"
                   "network_agent text NULL,"
                   "last_time text NULL,"
                   "associate_work text NULL,"
                   "production text NULL,"
                   "operator text NULL,"
                   "threshold_info text NULL,"
                   "test_status text NULL,"
                   "network_operator text NULL,"
                   "start_time_system text NULL,"
                   "clear_time_system text NULL);"
                   )
    for row in data:
        for i in range(len(row)):
            if row[i] == '':
                row[i] = None
        cursor.execute("INSERT INTO cnio.alarm_zx_hour VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                   [row[0], row[1], row[2], row[3], row[4], row[5],row[6], row[7], row[8], row[9], row[10], row[11], row[12], row[13], row[14], row[15],row[16], row[17], row[18], row[19], row[20], row[21], row[22], row[23], row[24], row[25],row[26], row[27], row[28], row[29], row[30], row[31], row[32], row[33], row[34], row[35],row[36], row[37], row[38], row[39], row[40], row[41], row[42], row[43], row[44], row[45],row[46], row[47], row[48], row[49], row[50], row[51], row[52], row[53], row[54]])

    print('Done inserting data...')
    cursor.close()
    database_postgres.commit()
    database_postgres.close()

def postgre_execute():
    path = r"C:\Users\Lenovo\Desktop\工作\需求分析\15. 小时天周字段调整\告警小时天周汇聚\华为中兴导入同一张表\alarm_4G_20220919\中兴4G历史告警0919.csv"
    data = pd.read_csv(path, encoding='gbk')
    print("读取csv文件成功")


    engine = create_engine('postgresql://expert-system:YJY_exp#exp502@10.1.77.51:5432/expert-system')
    data.to_sql("alarm_zx_hour", engine, schema="cnio", index=False, if_exists='append')
    print("success")

if __name__ == '__main__':
#    postgre_execute()
    postgre_create()
    print("success")