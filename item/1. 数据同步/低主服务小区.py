import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import datetime
import math
import multiprocessing as mp
import time
from io import StringIO



def get_problem_list():
    conn = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502",
                            host="10.1.77.51", port="5432")

    print("Postgres Connection success")
    cur = conn.cursor()
    sql_problem_data = """
    select
        c.*,
        d.max_dist
    from
        (
        select
            a.* ,
            b.distance * 1000 as avgsitedist
        from
            (
            select
                *
            from
                evaluation.low_primary_coverage a
            where
                data_date = current_date-2) a
        left join cnio.lte_site_structure b on
            a.eci / 256 = b.siteid) c
    left join 
    (
        select
            eci,
            max(dist) as max_dist
        from
            cnio.trace_mr_grid_cell_all tmgca
        where
            servingrsrp>-115
            and trace_date = current_date-2
            and servingsample>0
            and totalsample>0
            and (servingsample / totalsample)>0.3
        group by
            eci ) d on
        c.eci = d.eci
    ;
    """
    cur.execute(sql_problem_data)
    problem_list = cur.fetchall()
    if len(problem_list) == 0:
        pass
    else:
        problem_list_pd = pd.DataFrame(problem_list)
        des = cur.description
        colname = []
        for item in des:
            colname.append(item[0])
        problem_list_pd.columns = colname


    cur.close()
    conn.close()
    print("Postgres Connection release ")

    return problem_list_pd


def get_louyu():
    conn = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502",
                            host="10.1.77.51", port="5432")

    print("Postgres Connection success")
    cur = conn.cursor()
    sql_louyu= "select title as building_name,path as border,height as building_height from cnio.louyu_list ll"
    cur.execute(sql_louyu)
    louyu_list = cur.fetchall()
    if len(louyu_list) == 0:
        pass
    else:
        louyu_list_pd = pd.DataFrame(louyu_list)
        des = cur.description
        colname = []
        for item in des:
            colname.append(item[0])
        louyu_list_pd.columns = colname

    cur.close()
    conn.close()
    louyu= center_lonlat(louyu_list_pd)
    print("Postgres Connection release ")

    return louyu


def calc_distance(lola1,lola2):
    lat1 = lola1[1]
    lon1 = lola1[0]
    lat2 = lola2[1]
    lon2 = lola2[0]
    R=6371000
    lat1_rad = lat1 * math.pi / 180
    lon1_rad = lon1 * math.pi / 180
    lat2_rad = lat2 * math.pi / 180
    lon2_rad = lon2 * math.pi / 180
    d = math.sqrt(math.pow(math.cos((lat1_rad + lat2_rad) / 2) * R * (lon1_rad - lon2_rad), 2) +
                  math.pow(R * (lat1_rad - lat2_rad), 2))
    if d < 10000:
        return float(d)
    else:
        delta_lat = lat2_rad - lat1_rad
        delta_lon = lon2_rad - lon1_rad
        a = math.sin(delta_lat / 2) * math.sin(delta_lat / 2) + \
            math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) * math.sin(delta_lon / 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        return float(R * c)


def data_upload(data):
    output = StringIO()
    data.to_csv(output, sep='$', index=False, header=False)
    output = output.getvalue()
    conn = psycopg2.connect(host="10.1.77.51", user="expert-system", password="YJY_exp#exp502",
                            database="expert-system", port="5432", options="-c search_path=diagnosis")
    cur = conn.cursor()
    cur.copy_from(StringIO(output), "low_primary_diagnosis", sep='$', null='')
    conn.commit()
    cur.close()
    conn.close()
    print("low_primary_diagnosis sync: success")

def center_lonlat(data):
    data["center_lon"]=-1
    data["center_lat"]=-1
    for i in data.index:
        temp = data.loc[i,"border"]
        temp = temp.replace("((","").replace("))","")
        temp_list = temp.split("),(")
        lon=0
        lat=0
        for j in range(len(temp_list)):
            lonlat=temp_list[j].split(",")
            lon += float(lonlat[0])
            lat += float(lonlat[1])
        data["center_lon"] = lon/len(temp_list)
        data["center_lat"] = lat/len(temp_list)
    return data

##两点间方向角计算
def calc_azimuth(lola1,lola2):
    lat1 = lola1[1]
    lon1 = lola1[0]
    lat2 = lola2[1]
    lon2 = lola2[0]
    lat1_rad = lat1 * math.pi / 180
    lon1_rad = lon1 * math.pi / 180
    lat2_rad = lat2 * math.pi / 180
    lon2_rad = lon2 * math.pi / 180
    y = math.sin(lon2_rad - lon1_rad) * math.cos(lat2_rad)
    x = math.cos(lat1_rad) * math.sin(lat2_rad) - \
        math.sin(lat1_rad) * math.cos(lat2_rad) * math.cos(lon2_rad - lon1_rad)
    brng = math.atan2(y, x) * 180 / math.pi
    return float((brng + 360.0) % 360.0)


def louyu_flag(problem_list):
    louyu_list  = get_louyu()
    problem_list["building_flag"]=None
    problem_list["building_dist"] = -1
    problem_list["building_name"] = None
    problem_list["building_height"] = -1
    for i in problem_list.index:
        for j in louyu_list.index:
            try:
                cell_lon=problem_list.loc[i,"celllon"]
                cell_lat=problem_list.loc[i,"celllat"]
                building_lon = louyu_list.loc[j,"center_lon"]
                building_lat= louyu_list.loc[j,"center_lat"]
                dist = calc_distance([cell_lon,cell_lat],[building_lon,building_lat])
                if dist> 100:
                    continue
                else:
                    if math.fabs(calc_distance([cell_lon,cell_lat],[building_lon,building_lat])-problem_list.loc[i,"azimuth"])<60 and\
                        float(louyu_list.loc[j,"building_height"] )> float(problem_list.loc[i, "height"])*0.8:
                        problem_list["building_flag"] = 1
                        problem_list["building_dist"] = dist
                        problem_list["building_name"] = louyu_list.loc[j,"building_name"]
                        problem_list["building_height"] = louyu_list.loc[j,"building_height"]
                        break
            except:
                continue
    return problem_list

def multi_process(problem_list):
    seed_num = 30
    pool = mp.Pool(seed_num)
    max_row_num= problem_list.shape[0]
    tasks = []
    step = math.ceil(max_row_num / seed_num)
    start_time = time.time()
    for i in range(seed_num):
        if (i + 1) * step >= max_row_num:
            row_start = i * step
            row_end = max_row_num
        else:
            row_start = i * step
            row_end = (i + 1) * step
        print(row_end - row_start)
        tasks.append(problem_list.iloc[row_start:row_end, :])

    results = [pool.apply_async(louyu_flag, args=[task]) for task in tasks]

    results = [p.get() for p in results]

    for i in range(len(results)):
        if i == 0:
            problem_output = results[0]
        else:
            problem_output = pd.concat([problem_output, results[i]])
    end_time = time.time()
    print("楼宇阻挡判断用时{0}分钟".format((end_time - start_time) / 60))
    return problem_output

def problem_diagnosis(data):
    data["classify"] = ""
    data["root_cause"] = ""
    data["action"] = ""
    for i in data.index:
        if data.loc[i,"building_flag"]==1:
            data.loc[i,"classify"] = "维护"
            data.loc[i,"root_cause"] = "建筑物阻挡"
            data.loc[i,"action"] = "站点搬迁"
        elif data.loc[i,"bestrsrp"] < -100:
            data.loc[i,"classify"] = "维护"
            data.loc[i,"root_cause"] = "疑似天馈质量下降"
            data.loc[i,"action"] = "排查天馈质量"
        elif data.loc[i,"tilt"] > 10 and data.loc[i,"max_dist"]< data.loc[i,"avgsitedist"]*0.5:
            data.loc[i,"classify"] = "优化"
            data.loc[i,"root_cause"] = "天线下倾角过大"
            data.loc[i,"action"] = "降低天线下倾角"
        else:
            data.loc[i,"classify"] = "-"
            data.loc[i,"root_cause"] = "信息不足无法判断根因"
            data.loc[i,"action"] = "回溯信令或者现场测试"
    return data

if __name__ == '__main__':
    global louyu_list
    problem_list = get_problem_list()
    problem_building = multi_process(problem_list)
    problem_diagnosis_output= problem_diagnosis(problem_building)
    #problem_diagnosis_output.to_csv("diagnosis.csv")
    data_upload(problem_diagnosis_output)
    print("ok")