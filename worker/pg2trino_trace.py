import psycopg2
import pandas as pd
import datetime
from sqlalchemy import create_engine
import sys
import trino

engine = create_engine("postgresql+psycopg2://trace-tables:YJY_tra#tra502@10.1.77.51:5432/trace-tables",
                       encoding='utf-8')

if __name__ == '__main__':
    print('hello, world')
    trace_date = datetime.date.today() + datetime.timedelta(days=-1)
    trace_date = trace_date.strftime('%Y%m%d')

    db_name = 'trace-pg'
    schema_name = 'public'
    table_name = 'siteinfo'

    if len(sys.argv) >= 2:
        table_name = sys.argv[1]

    if len(sys.argv) >= 3:
        trace_date = sys.argv[2]

    print("本次表名： {}".format(table_name))
    print("本次日期： {}".format(trace_date))
    sql = """
        select 
            taskid 
        from 
            sk_task st 
        where 
            file_filter = '{}' 
        order by 
            begin_time desc 
        limit 1
    """.format(trace_date)

    task_id_df = pd.read_sql_query(sql, con=engine)

    if len(task_id_df) == 0:
        raise Exception("trace数据库中无{}日数据".format(trace_date))

    task_id = task_id_df.iloc[0, 0]
    print('task_id: {}'.format(task_id))

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
    """.format(db_name, schema_name, 'task_' + str(task_id) + '_' + table_name)

    conn = trino.dbapi.connect(
        host='trino.cnio.local',
        port=80,
        user='root',
        catalog='iceberg',
        schema='cnio',
    )
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    df = pd.DataFrame(rows)
    columns_list = df.iloc[:, 2].values

    columns_list_str = ",".join(columns_list)

    df['target'] = df.iloc[:, 2] + ' ' + df.iloc[:, 3]
    columns_type_list = ','.join(df['target'].values)

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS iceberg.cnio.trace_{} (
            {},
            trace_date varchar(50)
        ) WITH (
            partitioning = ARRAY['trace_date']
        )
    """.format(table_name, columns_type_list)

    print("创建表sql:\n{}".format(create_table_sql))

    cur = conn.cursor()
    cur.execute(create_table_sql)
    result = cur.fetchall()
    print("创建表结果：{}".format(result))

    insert_sql = """
    INSERT INTO "iceberg".cnio.trace_{}
    (
        {}, trace_date
    )
    select {}, '{}' as trace_date from "trace-pg".public.task_{}_{}
    """.format(table_name, columns_list_str, columns_list_str, trace_date, task_id, table_name)

    print("插入表数据sql:\n{}".format(insert_sql))

    conn = trino.dbapi.connect(
        host='trino.cnio.local',
        port=80,
        user='root',
        catalog='iceberg',
        schema='cnio',
    )
    cur = conn.cursor()
    cur.execute(insert_sql)
    result = cur.fetchall()
    print("插入表数据结果：{}".format(result))
