import trino
from trino import transaction
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

def trino_conn():
    conn = trino.dbapi.connect(
        host = 'trino.cnio.local',
        port = 80,
        user = 'root',
        catalog = 'hive',
        schema = 'cnio',
        isolation_level = transaction.IsolationLevel.READ_COMMITTED
    )
    return conn
def trino_execute(conn,sql):
    cursor = conn.cursor()
    cursor.execute(sql)
    des = cursor.description
    #data = cursor.fetchmany(size = 3)
    data = cursor.fetchall()
    data = pd.DataFrame(data)

    colname=[]
    for item in des:
        colname.append(item[0])
    data.columns=colname

    cursor.close()
    conn.close()
    return data

# trino://root@trino.cnio.local:80/hive/cnio
def trino_expert(data1):
    engine = create_engine('trino://root@trino.cnio.local:80/hive/cnio')
    data1.to_sql("siteinfo",engine,schema="cnio",  index=False, if_exists='append')


if __name__ == '__main__':
    conn = trino_conn()
    sql_hour  = '''  select * from hive.cnio.siteinfo limit 10 '''
    data = trino_execute(conn,sql_hour)
    print("trino 连接成功")
    trino_expert(data)
    print("success")