import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import datetime as dt
from io import StringIO
import numpy as np

def postgre_execute():
    conn = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502",
                            host="10.1.77.44", port="5432")
    cur = conn.cursor()
    # 获取taskid
    sql_alarm = """ select * from cnio.fault_workorder
    """
    cur.execute(sql_alarm)
    date = cur.fetchall()

    if len(date) != 0:
        fault_workorder_pd = pd.DataFrame(date)
        des = cur.description
        colname = []
        for item in des:
            colname.append(item[0])
        fault_workorder_pd.columns = colname

    cur.close()
    conn.close()

    return fault_workorder_pd

def postgre_weakcoverage(fault_workorder):
    conn = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502",
                            host="10.1.77.44", port="5432")
    cur = conn.cursor()
    # 获取taskid

    if len(fault_workorder) !=0:
        cul  =  fault_workorder[['rel_id','comments','tuning_action','order_sorting','root_cause','fault_subclass','fault_catalog']]
      #  print(cul)
        cul["flag_subclass"] =  cul["fault_subclass"].apply(lambda x: 1 if "弱覆盖" in x else 0)
        # cul["flag_catalog"] = cul["fault_catalog"].apply(lambda x: 1 if "区域类" in x else 0)
        # flag_catalog = cul[cul["flag_catalog"] == 1]
        flag_subclass = cul[cul["flag_subclass"] == 1]
        flag_subclass.index = range(flag_subclass.shape[0]) # 遍历行数'
        culupdate = fault_workorder[['comments','tuning_action','order_sorting','root_cause']]

        for i in flag_subclass.index :
            for j in culupdate.columns:
                sql_update = '''
                    UPDATE cnio.fault_workorder
         SET %s = CASE rel_id
             WHEN %s THEN (select %s from cnio.poorcoverage_area_judgement where cast(areaid as integer) = %s)
         END
    WHERE rel_id IN (%s) and fault_catalog = '区域类' and fault_subclass = '弱覆盖'
                '''%(j,i,j,i,i)

        cur.execute(sql_update)
        cur.fetchall()


        culs = flag_subclass[['comments','tuning_action','order_sorting','root_cause']]

        print(culs)

        cur.close()
        conn.close()
if __name__ == '__main__':
    fault_workorder =  postgre_execute()
    postgre_weakcoverage(fault_workorder)
    print("success")