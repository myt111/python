from datetime import datetime
import numpy as np
from clickhouse_sqlalchemy import make_session
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
from io import StringIO
import trino

if __name__ == '__main__':
    print('hello, world')

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
    columns_list['type'] = df.iloc[:, 3] .apply(lambda x: postgre_clickhouse_type_replace(x))
    columns_list['column'] = columns_list['column_name'] + ' ' + columns_list['type']

    #print(columns_list)

    columns_list_str = ",".join(columns_list['column'].values)

    df['target'] = df.iloc[:, 2] + ' ' + df.iloc[:, 3]
    columns_type_list_postgre = ','.join(df['target'].values)

    print(columns_list_str)
    print(columns_type_list_postgre)

    create_clickhouse_sql = """
        CREATE TABLE if not exists {} (
            {}
        ) ENGINE = MergeTree()
        partition by toYYYYMMDD({})
        ORDER BY {};
    """.format(table_name, columns_list_str, partition_name, primary_key)

    print(create_clickhouse_sql)


    from clickhouse_driver import Client

    # client = Client(host='10.1.77.51', port=9008, user='default', database='default', password='YJY_cu#unicom502')
    # client.execute(create_clickhouse_sql)

    insert_sql = """
        INSERT INTO "clickhouse".default.{}
        (
            {}
        )
        select {} from "{}".{}.{} 
    """.format(table_name, ','.join(columns_list['column_name'].values),','.join(columns_list['column_name'].values), "expert-pg", schema_name,
               table_name)

    print(insert_sql)

    conn = trino.dbapi.connect(
        host='trino.cnio.local',
        port=80,
        user='root',
        catalog='clickhouse',
        schema='default',
    )

    cur = conn.cursor()
    cur.execute(insert_sql)
    rows = cur.fetchall()
    df = pd.DataFrame(rows)
    print(df)
