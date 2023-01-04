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
    sql_alarm = """ select * from cnio.cell_weakcoverageall
    """
    cur.execute(sql_alarm)
    date = cur.fetchall()

    if len(date) != 0:
        weakcoverage_all_pd = pd.DataFrame(date)
        des = cur.description
        colname = []
        for item in des:
            colname.append(item[0])
        weakcoverage_all_pd.columns = colname

 #       print(weakcoverage_all_pd)


    cur.close()
    conn.close()

    return weakcoverage_all_pd

def postgre_weakcoverage(weakcoverage):
    if len(weakcoverage) != 0:
        if(weakcoverage['alarm_cell_num'] is not None):
            weakcoverage.iloc
            weakcoverdate =  weakcoverage[['alarm_cell_num','ldist_cell_ratio','sdist_cell_ratio','overshooting_cell_ratio']]

    df = pd.DataFrame(weakcoverage)
    colname = []
    # 获取列表头
    for i in df.columns:
        colname.append(i)
    # 添加到列表中
    df.columns = colname
    #   print(df)
    #  print("success df")
    alarm_cell_num = df["alarm_cell_num"]
    #  print(alarm_cell_num)

    # areaid                                         17
    # lat                            113.82997833333333
    # lon                             34.64530250000001

    #print(df.iloc[0]) # 拿到一行的值
    #print(df.loc[0])
    #  shape = df.shape[1]
    # print(shape) 22列
    # range 步长，shape[0] 对行进行计数，shape[1] 对列进行计数

    for i in range(df.shape[0]):
        print(i)


if __name__ == '__main__':
    weakcoverage =  postgre_execute()
    postgre_weakcoverage(weakcoverage)
    print("success")