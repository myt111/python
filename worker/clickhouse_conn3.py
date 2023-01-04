from datetime import datetime
import numpy as np
from clickhouse_sqlalchemy import make_session
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
from io import StringIO


# def to_sqldatatype(data):
#     def datatype_map(value):
#         if isinstance(value, int) or isinstance(value, np.int64):
#             datatype = 'Int32'
#         elif isinstance(value, float) or isinstance(value, np.float64):
#             datatype = 'Float32'
#         elif isinstance(value, np.datetime64) or isinstance(value, datetime):
#             datatype = 'Datetime64'
#         elif isinstance(value, list) or isinstance(value, np.ndarray):
#             datatype = 'Array(String)'
#         else:
#             datatype = 'String'
#         return datatype
#
#     data = data.copy()
#     temp = data.iloc[0]
#     datatypesql = ''
#     for i, j in zip(temp.index, temp.values):
#         datatype = datatype_map(j)
#         datatypesql = datatypesql + i + ' ' + datatype if datatypesql == '' else datatypesql + ',\n' + i + ' ' + datatype
#         if isinstance(j, int) or isinstance(j, np.int64) or isinstance(j, float) or isinstance(j, np.float64):
#             data[i] = data[i].fillna(np.nan).copy()
#         else:
#             data[i] = data[i].fillna('')
#     return datatypesql

def postgre_execute():
    conn = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502",
                            host="10.1.77.51", port="5432")
    cur = conn.cursor()
    # 获取taskid
    sql_alarm = """
            select siteid as enbid from "expert-system".searching.alarm_hz_day_1 limit 10
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

    print(create_clickhouse_sql)

    from clickhouse_driver import Client

    client = Client(host='10.1.77.51', port=9008, user='default', database='default', password='YJY_cu#unicom502')

    print(data)

    df = pd.DataFrame(data)
    datalist = []
    # dicts = {'user_id': int(num[0]), 'item_id': int(num[1])}

    # columns = list(dictdf.keys())
    #
    # for i in range(len(columns)):
    #     col_name = columns[i]
    #     col_type = dictdf[col_name]
    #     df[col_name] = df[col_name]
    #     if 'Date' in col_type:
    #         df[col_name] = pd.to_datetime(df[col_name])
    col = ','.join(data.columns.tolist())
    print(col)
    data = df.to_dict('records')

    client.execute(f"INSERT INTO alarm_hz_day_1 ( enbid ) VALUES" , data,  types_check=True)

    print("data transmit success")


if __name__ == '__main__':
    data = postgre_execute()
    updata(data)
    print("success")

