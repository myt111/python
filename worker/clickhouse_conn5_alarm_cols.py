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
    cur = conn.cursor()
    # 获取taskid
    sql_alarm = """
            select data_time as searching_datetime,siteid as enbid,cellid,eci,vendor,total_num,important_alarm_num,urgent_alarm_num,less_important_alarm_num,hint_alarm_num,top1_alarm_name,top1_alarm_num,top2_alarm_name,top2_alarm_num,top3_alarm_name,top3_alarm_num from "expert-system".searching.alarm_hz_day_1 limit 10
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
    conf = {
        "user": "default",
        "password": "YJY_cu#unicom502",
        "server_host": "10.1.77.51",
        "port": "8123",
        "db": "default"
    }
    clickhouse_postgre_types_dict = {
        'String': 'varchar',
        'Date': 'date',
        'Int32': 'integer',
        'Float64': 'double'
    }

    postgre_clickhouse_types_dict = {
        'TEXT': 'String',
        'DOUBLE PRECISION': 'Float64',
        'SMALLINT': 'Int16',
        'INTEGER': 'Int32',
        'BIGINT': 'Int64',
        'REAL': 'Float32',
        'DOUBLE': 'Float64',
        'NUMERIC': 'Decimal(38, 5)',
        'VARCHAR': 'String',
        'CHAR': 'String',
        'CHARACTER': 'String',
        'DATE': 'Date',
        'CHARACTER VARYING': 'varchar(64)',
        'UUID': 'UUID'
    }

    db_name = 'expert-system'
    schema_name = 'searching'
    table_name = 'trace_lte_cell_sum_day'

    partition_name = 'data_date'
    primary_key = 'eci'

    sql = """
            select 
                table_catalog, 
                table_name, 
                column_name, 
                data_type
            from 
                "{}".information_schema.columns 
            where 
                table_schema = '{}' and
                table_name = '{}'
        """.format(db_name, schema_name, table_name)

    def postgre_clickhouse_type_replace(postgre_type: str) -> str:
        return postgre_clickhouse_types_dict.get(str(postgre_type).upper())

    conn = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502",
                            host="10.1.77.51", port="5432")

    cur = conn.cursor()

    cur.execute(sql)
    rows = cur.fetchall()
    df = pd.DataFrame(rows)
    columns_list = pd.DataFrame()
    columns_list['column_name'] = df.iloc[:, 2]
    columns_list['type'] = df.iloc[:, 3].apply(lambda x: postgre_clickhouse_type_replace(x))
    columns_list['column'] = columns_list['column_name'] + ' ' + columns_list['type']

    columns_list_str = ",".join(columns_list['column'].values)

    df['target'] = df.iloc[:, 2] + ' ' + df.iloc[:, 3]
    columns_type_list_postgre = ','.join(df['target'].values)

    print(columns_list_str)
    print(columns_type_list_postgre)
    #
    create_clickhouse_sql = """
           CREATE TABLE if not exists {} (
               {}
           ) ENGINE = MergeTree()
           partition by toYYYYMMDD({})
           ORDER BY {};
       """.format(table_name, columns_list_str, partition_name, primary_key)

    #print(create_clickhouse_sql)

    from clickhouse_driver import Client

    client = Client(host='10.1.77.51', port=9008, user='default', database='default', password='YJY_cu#unicom502')
    df = pd.DataFrame(data)
    cols = ','.join(data.columns.tolist())
    print(cols)
    data = df.to_dict('records')
    client.execute(f"INSERT INTO alarm_hz_day_1 ( {cols} ) VALUES" , data,  types_check=True)
    print("data transmit success")

if __name__ == '__main__':
    data = postgre_execute()
    updata(data)
    print("success")

