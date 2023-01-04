import minio
import trino
from trino import transaction
import os
import json
import datetime as dt
import time
from pathlib import Path
import os
import pandas as pd

def upload_minio(bucket: str, file_name: str, file_path):
    client = minio.Minio(**MINIO_CONF)
    client.fput_object(bucket_name=bucket, object_name=file_name,
                       file_path=file_path
                       # # content_type='application/zip'
                       # content_type='application/text'
                       )


# def minio_con(bucket_name):
#     try:
#         client = minio.Minio(**MINIO_CONF)
#         found = client.bucket_exists(bucket_name)
#     except minio.S3Error as e:
#         print("error:", e)
#     return found


# ##鏁版嵁
# def latest_minio_find(bucket: str, object: str):
#     client = minio.Minio(**MINIO_CONF)
#     if not client.bucket_exists(bucket):
#         return None
#     data = client.get_object(bucket, object)
#     output = data.data.decode('utf-8')
#     return output

# conn = trino.dbapi.connect(
#         host = '133.160.191.113',
#         port = 32470,
#         user = 'root',
#         catalog = 'hive',
#         schema = 'cnio',


def trino_partition(time_partition):
    # 寤虹珛杩炴帴
    conn = trino.dbapi.connect(
        host='133.160.191.100',
        port=32470,
        user='root',
        catalog='hive',
        schema='cnio',
        isolation_level = transaction.IsolationLevel.READ_COMMITTED
    )
    cursor = conn.cursor()
    sql = """
    call hive.system.create_empty_partition(
    schema_name => 'cnio', 
    table_name => 'pm_4g', 
    partition_columns =>ARRAY['pm_date'],
    partition_values =>ARRAY['%s'])
    """ % time_partition
    cursor.execute(sql)
    res = cursor.fetchall()

    cursor.close()
    conn.close()


def get_time():
    now_time = dt.datetime.now()
    previous_time = now_time + dt.timedelta()
    previous_hour = previous_time.strftime('%H')
    previous_date = previous_time.strftime('%Y%m%d')

    return previous_date, previous_hour

def get_time_list(days_num):
    now_time = dt.datetime.now()
    today_date = now_time.strftime('%Y%m%d')
    min_time = now_time + dt.timedelta(days= days_num)
    min_date  = min_time.strftime('%Y%m%d')
    min_time  = dt.datetime.strptime(min_date,'%Y%m%d')

    time_list =[]
    while now_time>=min_time:
        temp_hour = now_time.strftime('%H')
        temp_date = now_time.strftime('%Y%m%d')
        time_list.append([temp_date,temp_hour])
        now_time = now_time + dt.timedelta(hours=-1)

    date_list=(int(min_date),int(today_date))
    return date_list,time_list

def upload_hour_list(date_list):
    # 寤虹珛杩炴帴
    conn = trino.dbapi.connect(
        host='133.160.191.100',
        port=32470,
        user='root',
        catalog='hive',
        schema='cnio',
        isolation_level = transaction.IsolationLevel.READ_COMMITTED
    )
    cursor = conn.cursor()
    sql = """
        select
        distinct to_char(cast(start_time as timestamp),'yyyymmddhh24') as hour_time
    from
        hive.cnio.pm_4g
    where
        cast(pm_date as int) between %s and %s  
    """ % date_list
    cursor.execute(sql)
    data = cursor.fetchall()

    if len(data) != 0:
        data=pd.DataFrame(data)
        data.columns = ["hour_time"]
        data = data["hour_time"].tolist()
        print("数据时间读取完毕")

    else:
        print("数据时间读取失败")
        data = []
    
    print("upload hour {0}".format(data))
    cursor.close()
    conn.close()
    return data

def unload_hour(time_list,load_data_hour):
    unload_hour_list=[]
    for i in range(len(time_list)):
        print("time_list {0}".format(time_list[i]))
        if time_list[i][0]+time_list[i][1] in load_data_hour:
            continue
        else:
            unload_hour_list.append(time_list[i])
    return unload_hour_list

def data_upload_minio(unload_hour_list):
    for i in range(len(unload_hour_list)):
        previous_date=unload_hour_list[i][0]
        previous_hour = unload_hour_list[i][1]
        
        pm_dir_path = "/bigdata/data_warehouse/zhengzhou/2_PM/4G"
        dir_path = pm_dir_path + "/" + previous_date
        print(dir_path)
    
        if os.path.exists(dir_path):
    
            file_path = dir_path + "/lte_pm_" + previous_date + previous_hour + ".csv"
    
            print(file_path)
            if os.path.exists(file_path):
                try:
                    trino_partition(previous_date)
                    print("建分区成功")
                    upload_minio(bucket="warehouse", file_name="cnio.db/pm_4g/pm_date=" + previous_date + "/" +
                                                               "lte_pm_" + previous_date + previous_hour + ".csv",
                                 file_path=file_path)
                    print("数据同步成功")
                except:
                    upload_minio(bucket="warehouse", file_name="cnio.db/pm_4g/pm_date=" + previous_date + "/" +
                                                               "lte_pm_" + previous_date + previous_hour + ".csv",
                                 file_path=file_path)
                    print("数据同步成功")
            else:
                print("路径不存在")

if __name__ == "__main__":
    ## MinIO账密
    MINIO_CONF = {
        'endpoint': '133.160.191.120:19999',
        'access_key': '76bc159d-1826-df48-5334-cd101ec184bb',
        'secret_key': '4da591f2-bd12-42d0-d918-02531e599d80',
        'secure': False
    }
    days_num = ${delay_days}
    date_list,time_list=  get_time_list(days_num)
    load_data_hour = upload_hour_list(date_list)
    unload_hour_list=unload_hour(time_list, load_data_hour)
    print("unload_hour_list {0}".format(unload_hour_list))
    data_upload_minio(unload_hour_list)

    

