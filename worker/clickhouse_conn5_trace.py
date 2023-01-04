from datetime import datetime
import numpy as np
from clickhouse_sqlalchemy import make_session
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
from io import StringIO

def postgre_execute():
    conn = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502",
                            host="10.1.77.51", port="5432")
    # conn = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502",
    #                         host="133.160.191.111", port="5432")

    cur = conn.cursor()
    # 获取taskid
    sql_alarm = """
            select * from "expert-system".searching.trace_lte_cell_sum_day
    """
    cur.execute(sql_alarm)
    data = cur.fetchall()
    data = pd.DataFrame(data)
    des = cur.description
    colname = []
    for item in des:
        colname.append(item[0])
    data.columns = colname
    cur.close()
    conn.close()
    print("pg conn")
    return data

def updata(data):

    from clickhouse_driver import Client

    client = Client(host='10.1.77.51', port=9008, user='default', database='default', password='YJY_cu#unicom502')
    # client = Client(host='133.160.191.107', port=9000, user='sk', database='default', password='trace@2022.wyy')

    df = pd.DataFrame(data)
    cols = ','.join(data.columns.tolist())
    print(cols)
    data = df.to_dict('records')
    client.execute(f"INSERT INTO trace_lte_cell_sum_day_1 ( {cols} ) VALUES" , data,  types_check=True)
    print("data transmit success")

if __name__ == '__main__':
    data = postgre_execute()
    updata(data)
    print("success")

