import pandas
import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import datetime as dt
import zipfile
from io import StringIO

def data_upload(data,table_name,schema_name):
    engine = create_engine('postgresql://expert-system:YJY_exp#exp502@133.160.191.111:5432/expert-system')
    data.to_sql(table_name,engine,schema=schema_name,  index=False, if_exists='append',method='multi')

def data_load(data, table, expert_db_account):
    #data.to_csv("test.csv",index=False)
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

def csv_read(path):
    ##解码是否正常标记 1 完成解码 -1 解码错误
    flag = 0
    ##尝试utf8解码
    try:
        data = pd.read_csv(path, encoding='utf-8')
        pd_data = pd.DataFrame(data)
        flag = 1
    except:
        print("数据读取异常")
        flag = -1
    ##尝试gbk解码
    if flag == -1:
        try:
            data = pd.read_csv(path, encoding='gbk')
            pd_data = pd.DataFrame(data)
            flag = 1
        except:
            print("数据读取异常")
            flag = -1
    ##尝试ansi解码
    if flag == -1:
        try:
            data = pd.read_csv(path, encoding='ansi')
            pd_data = pd.DataFrame(data)
            flag = 1
        except:
            print("数据读取异常")
            pd_data = []
            flag = -1
    return pd_data



def get_date():
    today = dt.datetime.today()
    today_date = today.strftime('%Y%m%d')

    return today_date


if __name__=="__main__":
    # expert_db_account = {"database": "expert-system", "user": "expert-system", "password": "YJY_exp#exp502",
    #                      "host": "10.1.77.51", "port": "5432", "options": "-c search_path=cnio"}
    root = r"/bigdata/data_warehouse/zhengzhou/6_ALARM/4G/"
    today=get_date()
    #today='20221121'
    path = root+today
    print(path)
    if os.path.exists(path):
        frzip = zipfile.ZipFile(path+"/"+"alarm_4G_"+today+".zip", 'r', zipfile.ZIP_DEFLATED)
        for filename in frzip.namelist():
            frzip.extract(filename,path)
            newname = filename.encode('cp437').decode('gbk')
            print('extract file:', newname)
            if "中兴" in newname:
                zte_data = csv_read(path+"/"+filename)
                if len(zte_data):
                    #zte_data = pd.DataFrame(zte_data)
                    zte_data.columns=["rootalarm_flag",	"ack_status",	"alarm_level",	"network",	"locate_info",
                                      "system_type",	"alarm_name",	"start_time",	"network_type",	"alarm_type",
                                      "alarm_reason",	"addition_text",	"admc",	"clear_time",	"count",	"alarm_object_name",
                                      "alarm_object_type",	"single_board",	"enodebid",	"alarm_source",	"alarm_object_id",
                                      "alarm_object_dn",	"alarm_id_standard",	"network_type2",	"alarm_flag",
                                      "influence_network",	"influence_network_locate",	"revise_time",	"addition",	"confirm_object",
                                      "confirm_system",	"confirm_time",	"confirm_info",	"clear_object",	"clear_system",	"recovery_method",
                                      "clear_info",	"alarm_notes",	"notes_object",	"notes_system",	"notes_time",	"alarm_serial_number",
                                      "network_ip",	"link",	"network_group",	"network_agent",	"last_time",	"associate_work",
                                      "production",	"operator",	"threshold_info",	"test_status",	"network_operator",
                                      "start_time_system",	"clear_time_system"]
                    data_upload(zte_data,"lte_alarm_detail_zte","cnio")
                os.remove(path+"/"+filename)
                print("中兴告警文件已删除")
            elif "华为" in newname:
                hw_data = csv_read(path+"/"+filename)
                if len(hw_data):
                    #hw_data = pd.DataFrame(hw_data)
                    hw_data.columns=["rootalarm_flag",	"alarm_level",	"alarm_id",	"alarm_name",
                                     "network_type",	"alarm_source",	"mo_object",	"locate_info",	"start_time",
                                     "clear_time",	"confirm_time",	"clear_object",	"confirm_object",	"clear_status",
                                     "rru",	"cell",	"ack_status",	"bbu",	"enodebid",	"gnodebid",	"journal_serial_number",
                                     "custom_identity",	"alarm_serial_number",	"addition",	"maintenance_status",	"subnet"]
                    data_upload(hw_data,"lte_alarm_detail_hw","cnio")
                    print("华为告警入库结束")
                os.remove(path+"/"+filename)
                print("华为告警文件已删除")
        print("告警记录已完成入库")
    else:
        print("路径不存在，无告警记录入库")


