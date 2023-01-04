import pandas
import os
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import datetime as dt
import zipfile
from io import StringIO


def data_upload(data, table_name, schema_name):
    engine = create_engine('postgresql://expert-system:YJY_exp#exp502@133.160.191.111:5432/expert-system')
    # engine = create_engine('postgresql://expert-system:YJY_exp#exp502@133.160.191.111:5432/expert-system')
    data.to_sql(table_name, engine, schema=schema_name, index=False, if_exists='append',method='multi')


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

def covertype_convert(x):
    if x=="宏站":
        return 0
    elif x=="室分":
        return 1
    elif x=="室分（楼顶对打）":
        return 2
    else:
        return 3

def tilt_process(x):
    x=str(x)
    if x=="无电子下倾角" or x=="电子电调" or x=="电调仪不识别"  or x=="无机械下倾角"  or x=="无机械倾角" :
        return 0
    elif "/" in x:
        x_list = x.split("/")
        return int(round(float(x_list[0]),0))
    else:
        return int(round(float(x),0))

def azimuth_process(x):
    x = str(x)
    if  "/" in x:
        x_list = x.split("/")
        return int(round(float(x_list[0]),0))
    else:
        return int(round(float(x),0))
def height_process(x):
    x = str(x)
    if x=="景观塔" or x=="单管塔":
        return 20
    elif  "/" in x:
        x_list = x.split("/")
        return int(round(float(x_list[0]),0))
    else:
        return int(round(float(x),0))

def process(content):
    if len(content) != 0:
        ## lte_siteinfo 入库
        content["optimize_grid"] = None
        content["cellindex"] = content[["eNodeBID", "CELLID"]].apply(
            lambda x: int(x["eNodeBID"]) * 256 + int(x["CELLID"]), axis=1)
        content["rattype"] = '4G'
        content["hbwd"] = content["网元类型"].apply(lambda x:360 if covertype_convert(x)==1 else 65)
        content["ci"] = content[["eNodeBID", "CELLID"]].apply(lambda x: int(x["eNodeBID"]) * 256 + int(x["CELLID"]),
                                                              axis=1)
        content["poweron"] = None
        content["indoor"] = content["网元类型"].apply(lambda x: covertype_convert(x))
        content["vbwd"] = None
        content["province"] = '河南'
        content["sversion"] =None
        content["sitedensity"] = None
        content["siteavgdist"] = None
        content["data_date"] = '%s' % today
        content["plan_site_name"] = None
        content["contract_area"] = None
        content["contractor"] = None
        content["local_community_flag"] = None

        content.columns = ["city", "vendor", "cellname", "tac", "sitename_z", "sitename", "siteid", "cellid",
                           "locellid", "cellname_z", "pci", "band", "channel", "bandwith",
                           "existed_min_root_sequence_index", "pilot_txpower", "pa", "pb", "sgw", "pgw", "mme",
                           "site_model", "district", "business_department", "end_office", "delivery_gird",
                           "rru_num", "site_type", "lon", "lat", "height", "altitude", "azimuth", "etilt",
                           "mtilt", "tilt", "antenna_place", "antenna_polarization_mode", "antenna_share",
                           "feeder_share", "feeder_model", "antenna_polarization_type", "antenna_vendor",
                           "is_etilt_flag", "antenna_type", "cabinet_type", "site_config", "antenna_txpower",
                           "share_with_operator", "cu_share_network_type", "cu_share_w_siteid",
                           "cu_share_w_sitename", "cu_share_g_siteid", "cu_share_g_sitename",
                           "cu_share_d_siteid", "cu_share_d_sitename", "covertype", "cover_area_type",
                           "site_address", "site_street", "site_community", "antenna_type_model", "scene",
                           "is_support_16qam", "is_support_64qam", "site_grade", "is_support_mimo",
                           "is_support_16qam_z", "is_support_dualcell", "is_support_mimo_z",
                           "is_support_64qam_z", "grid_grade", "level_1_scene_type", "level_2_scene_type",
                           "level_1_scene_border", "level_2_scene_border", "is_tac_flower",
                           "is_site_address_same_as_plan", "is_beautify_antenna", "is_height_same_as_plan",
                           "is_neighbour_check_base_centor", "microgrid_type", "area_grid", "remark",
                           "access_time", "is_maintenance", "capacity_expansion_cell", "rru_type",
                           "update_time", "box_number", "sector", "is_pulled_far", "scene_name", "optimize_grid",
                           "cellindex", "rattype", "hbwd", "ci", "poweron", "indoor", "vbwd", "province",
                           "sversion", "sitedensity", "siteavgdist", "data_date", "plan_site_name",
                           "contract_area", "contractor", "local_community_flag"]

        try:
            data_upload(content, "lte_siteinfo", "cnio")
            print("数据导入lte_siteinfo表单成功")
        except Exception as e:
            print("数据入库异常，异常：{0}".format(e))

        ### 同步cnio.siteinfo
        content_sub = content[
            ["cellindex", "rattype", "channel", "pci", "cellname", "lon", "lat", "azimuth", "hbwd",
             "siteid", "sitename", "cellid", "ci", "tac", "band", "poweron", "indoor", "vbwd", "height",
             "etilt", "mtilt", "vendor", "province", "city", "district", "scene", "covertype", "sversion",
             "sitedensity", "siteavgdist", "data_date", "cellname_z", "plan_site_name", "antenna_type",
             "contract_area", "contractor", "end_office", "delivery_gird", "site_type", "site_config",
             "local_community_flag", "optimize_grid"]]

        content_sub["etilt"]=content_sub["etilt"].apply(lambda x:tilt_process(x))
        content_sub["mtilt"]=content_sub["mtilt"].apply(lambda x:tilt_process(x))
        content_sub["azimuth"]=content_sub["azimuth"].apply(lambda x:azimuth_process(x))
        content_sub["height"]=content_sub["height"].apply(lambda x:height_process(x))


        try:
            data_upload(content_sub, "siteinfo", "cnio")
            print("数据导入siteinf成功")
        except Exception as e:
            print("数据导入siteinfo异常，异常：{0}".format(e))


if __name__ == "__main__":
    expert_db_account = {"database": "expert-system", "user": "expert-system", "password": "YJY_exp#exp502",
                         "host": "133.160.191.111", "port": "5432", "options": "-c search_path=cnio"}
    # path = r"/bigdata/data_warehouse/zhengzhou/1_CM/engineering_parameters/4G/cell/20221205"
    # root = "D:\\360安全浏览器下载\\"  #
    # expert_db_account = {"database": "expert-system", "user": "expert-system", "password": "YJY_exp#exp502",
    #                      "host": "10.1.77.51", "port": "5432", "options": "-c search_path=cnio"}
    root = r"/bigdata/data_warehouse/zhengzhou/1_CM/engineering_parameters/4G/cell/"
    today = get_date()
    #today ='20221205'
    path = root + today + "/"
    if os.path.exists(path):
        frzip = zipfile.ZipFile(path + "lte_siteinfo_" + today + ".zip", 'r', zipfile.ZIP_DEFLATED)
        for filename in frzip.namelist():
            if filename == 'lte_siteinfo_20221215/':
                #os.remove(path + filename)
                #print("文件已删除")
                continue
            if os.path.isdir(path + filename):
                os.remove(path + filename)
                print("文件已删除")
                continue
            frzip.extract(filename, path)
            newname = filename.encode('cp437').decode('gbk')
            content = pd.read_excel(path + filename)
            content = pd.DataFrame(content)
            content = content.loc[1:, ]
            process(content)
            os.remove(path  + filename)
            print("文件已删除")

    print("success")

