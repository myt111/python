import pandas
import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import datetime as dt
import zipfile
from io import StringIO


def data_upload(data, table_name, schema_name):
    engine = create_engine('postgresql://expert-system:YJY_exp#exp502@10.1.77.51:5432/expert-system')
    #engine = create_engine('postgresql://expert-system:YJY_exp#exp502@133.160.191.111:5432/expert-system')
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


def get_date():
    today = dt.datetime.today()
    today_date = today.strftime('%Y%m%d')

    return today_date



if __name__ == "__main__":
    expert_db_account = {"database": "expert-system", "user": "expert-system", "password": "YJY_exp#exp502",
                         "host": "10.1.77.51", "port": "5432", "options": "-c search_path=cnio"}
    #path = r"/bigdata/data_warehouse/zhengzhou/1_CM/engineering_parameters/4G/cell/20221205"
    path = "D:"    # expert_db_account = {"database": "expert-system", "user": "expert-system", "password": "YJY_exp#exp502",
    #                      "host": "10.1.77.51", "port": "5432", "options": "-c search_path=cnio"}
    #root = r"/bigdata/data_warehouse/zhengzhou/6_ALARM/4G/"
    today=get_date()
    #today='20221121'
    #path = root+today
    print(path)
    if os.path.exists(path):
        frzip = zipfile.ZipFile(path+"/"+"lte_siteinfo_20221205"+".zip", 'r', zipfile.ZIP_DEFLATED)
        for filename in frzip.namelist():
            frzip.extract(filename,path)
            newname = filename.encode('cp437').decode('gbk')
            print('extract file:', newname)

            # for root, dirs, files in os.walk(path):
            #     for file in files:
            content = pd.read_excel(path + filename)
            content = content.loc[1:, ]
            print(content.shape)
            #print(content)
            if len(content):
                #zte_data = pd.DataFrame(zte_data)

                # content["vendor"] = ''
                # content["optimize_grid"] = ''
                # # content["cellindex"] = content[["siteid", "cellid"]].apply(
                # #     lambda x: float(x["siteid"] * 256 + x["cellid"]),
                # #     axis=1)
                # content["cellindex"] = ''
                # content["rattype"] = '4G'
                # content["hbwd"] = ''
                # content[['siteid']] = content[['siteid']].astype(int)
                # # content[['cellid']] = content[['cellid']].astype(int)
                # # content["ci"] = content[["siteid", "cellid"]].apply(
                # #     lambda x: float(x["siteid"] * 256 + x["cellid"]),
                # #     axis=1)
                # content["ci"] = ''
                # content["poweron"] = ''
                # content["indoor"] = '宏站 0 室分 1 同时覆盖室内室外 2'
                # content["vbwd"] = ''
                # content["province"] = '河南'
                # content["sversion"] = ''
                # content["sitedensity"] = ''
                # content["siteavgdist"] = ''
                # content["data_date"] = ''
                # content["plan_site_name"] = ''
                # content["contract_area"] = ''
                # content["contractor"] = ''
                # content["local_community_flag"] = ''

                content.columns = ["city","vendor","cellname","tac","sitename_z","sitename","siteid","cellid","locellid","cellname_z","pci","band","channel","bandwith","existed_min_root_sequence_index","pilot_txpower","pa","pb","sgw","pgw","mme","site_model","district","business_department","end_office","delivery_gird","rru_num","site_type","lon","lat","height","altitude","azimuth","etilt","mtilt","tilt","antenna_place","antenna_polarization_mode","antenna_share","feeder_share","feeder_model","antenna_polarization_type","antenna_vendor","is_etilt_flag","antenna_type","cabinet_type","site_config","antenna_txpower","share_with_operator","cu_share_network_type","cu_share_w_siteid","cu_share_w_sitename","cu_share_g_siteid","cu_share_g_sitename","cu_share_d_siteid","cu_share_d_sitename","covertype","cover_area_type","site_address","site_street","site_community","antenna_type_model","scene","is_support_16qam","is_support_64qam","site_grade","is_support_mimo","is_support_16qam_z","is_support_dualcell","is_support_mimo_z","is_support_64qam_z","grid_grade","level_1_scene_type","level_2_scene_type","level_1_scene_border","level_2_scene_border","is_tac_flower","is_site_address_same_as_plan","is_beautify_antenna","is_height_same_as_plan","is_neighbour_check_base_centor","microgrid_type","area_grid","remark","access_time","is_maintenance","capacity_expansion_cell","rru_type","update_time","box_number","sector","is_far_away","scene_name"]
                data_upload(content,"lte_siteinfo_2","cnio")
            os.remove(path+"/"+filename)
            print("文件已删除")

    print("success")
