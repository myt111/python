import minio
import trino
from trino import transaction
import os
import json
import datetime
import time

##文件上传
def upload_minio(bucket: str,file_name: str, file_path):
    client = minio.Minio(**MINIO_CONF)
    client.fput_object(bucket_name=bucket, object_name=file_name,
                       file_path=file_path,
                       # content_type='application/zip'
                       content_type='application/text'
                       )

##文件删除
def delete_minio(bucket: str,file_name: str):
    client = minio.Minio(**MINIO_CONF)
    client.remove_object(bucket_name=bucket, object_name=file_name)

##文件下载
def get_minio(bucket: str,file_name: str, file_path):
    client = minio.Minio(**MINIO_CONF)
    client.fget_object(bucket_name=bucket, object_name=file_name,
                       file_path=file_path)

##获取文件数据写入指定位置
# def write_minio(bucket: str,file_name: str, file_path):
#     client = minio.Minio(**MINIO_CONF)
#     data = client.get_object(bucket_name=bucket, object_name=file_name)
#     with open("/opt/1demo.jpg", "wb") as fp:
#         for d in data.stream(1024):
#             fp.write(d)


def minio_con(bucket_name):
    try:
        client = minio.Minio(**MINIO_CONF)
        found = client.bucket_exists(bucket_name)
    except minio.S3Error as e:
        print("error:", e)
    return found


##数据
def latest_minio_find(bucket: str, object: str):
    client = minio.Minio(**MINIO_CONF)
    if not client.bucket_exists(bucket):
        return None
    data = client.get_object(bucket, object)
    output = data.data.decode('utf-8')
    return output


def trino_execute(time_partition):
    #建立连接
    conn = trino.dbapi.connect(
        host = 'trino.cnio.local',
        port = 80,
        user = 'root',
        catalog = 'hive',
        schema = 'cnio',
        #isolation_level = transaction.IsolationLevel.READ_COMMITTED
    )
    cursor = conn.cursor()
    sql  ="""
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


def trino_last_upload():
    #建立连接
    conn = trino.dbapi.connect(
        host = 'trino.cnio.local',
        port = 80,
        user = 'root',
        catalog = 'hive',
        schema = 'cnio',
        isolation_level = transaction.IsolationLevel.READ_COMMITTED
    )
    cursor = conn.cursor()
    sql  ="""
    select max(cast(pm_date as int)) from hive.cnio.pm_4g 
    """
    cursor.execute(sql)
    last_upload_time = cursor.fetchmany(size=1)

    cursor.close()
    conn.close()
    return last_upload_time


if __name__=="__main__":

    ##minio 连接
    MINIO_CONF = {
        'endpoint': '10.1.58.110:19999',
        'access_key': 'qiaojj91',
        'secret_key': '45a40b7f-18ed-4b4b-a491-7fcabe12bb65',
        'secure': False
    }

    #path = r"/bigdata/data_warehouse/zhengzhou/2_PM/4G"
    path = r"C:\Users\unicom\Desktop\data_file"

    if os.path.exists(path+"\\record.json"):
        record_content = json.loads(path+"/record.json")
        last_load_time = record_content["last_load_time"]
        load_fail_time = record_content["load_fail_time"]

        ##根据上次传输时间与当前时间差，将未同步数据同步
        now_time = datetime.datetime.now()
        time_delta = now_time-last_load_time
        time_delta_hour = time_delta.total_seconds()/3600

        for i in range(time_delta_hour):
            temp_time = last_load_time+datetime.timedelta(hours=i)

    else:
        for root, dirs, files in os.walk(path):
            if dirs!="":
                for time_partition in dirs:
                    trino_execute(time_partition)
            if files!="":
                for file in files:
                    if file.endswith(".csv"):
                        file_hour=file[(len(file)-14):(len(file)-4)]
                        file_date=file[(len(file)-14):(len(file)-6)]
                        upload_minio(bucket="warehouse", file_name="cnio.db/pm_4g/data_date="+file_date+"/"+file,
                                     file_path=root+"\\"+file)
        #文件上传结束，将最后上传时间记录
        last_upload_time = trino_last_upload()
        record = {"last_load_time":last_upload_time,"load_fail_time":""}
        with open(path+"\\"+"record.json", "w") as f:
            json.dump(record, f)
            print("加载入文件完成...")

    print("ok")



