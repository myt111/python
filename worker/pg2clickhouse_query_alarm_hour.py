import trino
import pandas as pd


if __name__ == '__main__':
    print('hello, world')

    clickhouse_trino_types_dict = {
        'String': 'varchar',
        'Date': 'date',
        'Int32': 'integer',
        'Float64': 'double'
    }

    trino_clickhouse_types_dict = {
        'BOOLEAN': 'UInt8',
        'TINYINT': 'Int8',
        'SMALLINT': 'Int16',
        'INTEGER': 'Int32',
        'BIGINT': 'Int64',
        'REAL': 'Float32',
        'DOUBLE': 'Float64',
        'DECIMAL(p, s)': 'Decimal(p, s)',
        'VARCHAR': 'String',
        'CHAR': 'String',
        'VARBINARY': 'String',
        'DATE': 'String',
        'TIMESTAMP(0)': 'DateTime',
        'UUID': 'UUID'
    }

    db_name = 'expert-system-pg'
    schema_name = 'searching'
    table_name = 'alarm_hz_hour'

    partition_name = 'data_time'
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

    conn = trino.dbapi.connect(
        host='trino.cnio.local',
        port=80,
        user='root',
        catalog='iceberg',
        schema='cnio',
    )

    def trino_clickhouse_type_replace(trino_type: str) -> str:
        if trino_type.upper().startswith('VARCHAR'):
            return trino_clickhouse_types_dict.get('VARCHAR')
        if trino_type.upper().startswith('DECIMAL'):
            return trino_type.capitalize()
        return trino_clickhouse_types_dict.get(str(trino_type).upper())

    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    df = pd.DataFrame(rows)
    columns_list = pd.DataFrame()
    columns_list['column_name'] = df.iloc[:, 2]
    columns_list['type'] = df.iloc[:, 3].apply(lambda x: trino_clickhouse_type_replace(x))
    columns_list['column'] = columns_list['column_name'] + ' ' + columns_list['type']

    print(columns_list)

    columns_list_str = ",".join(columns_list['column'].values)

    df['target'] = df.iloc[:, 2] + ' ' + df.iloc[:, 3]
    columns_type_list_4trino = ','.join(df['target'].values)

    print(columns_list_str)

    create_clickhouse_sql = """
        CREATE TABLE if not exists {} (
            {}
        ) ENGINE = MergeTree()
        partition by toDateTime(substring(toString({}), 1, 10))
        ORDER BY {};
    """.format(table_name, columns_list_str, partition_name, primary_key)

    print(create_clickhouse_sql)

    from clickhouse_driver import Client

    client = Client(host='10.1.77.51', port=9008, user='default', database='default', password='YJY_cu#unicom502')
    client.execute(create_clickhouse_sql)

    insert_sql = """
        INSERT INTO "clickhouse".default.{}
        (
            searching_datetime,siteid,cellid,eci,vendor,total_num,important_alarm_num,urgent_alarm_num,less_important_alarm_num,hint_alarm_num,top1_alarm_name,top1_alarm_num,top2_alarm_name,top2_alarm_num,top3_alarm_name,top3_alarm_num
        )
        select {} from "{}".{}.{} where urgent_alarm_num is not null and important_alarm_num is not null and less_important_alarm_num is not null
     and hint_alarm_num is not null
    """.format(table_name, columns_type_list_4trino, db_name, schema_name,
               table_name)
    # ','.join(columns_list['column_name'].values)
   #  insert_sql = """
   #           INSERT INTO "clickhouse".default.alarm_hz_hour
   #      (
   #          searching_datetime,siteid,cellid,eci,vendor,total_num,important_alarm_num,urgent_alarm_num,less_important_alarm_num,hint_alarm_num,top1_alarm_name,top1_alarm_num,top2_alarm_name,top2_alarm_num,top3_alarm_name,top3_alarm_num
   #      )
   #      select cast(substring(data_time, 1,10) as timestamp)  datetime,siteid integer,cellid double,eci double,vendor varchar,total_num integer,important_alarm_num double,urgent_alarm_num double,less_important_alarm_num double,hint_alarm_num double,top1_alarm_name varchar,top1_alarm_num double,top2_alarm_name varchar,top2_alarm_num double,top3_alarm_name varchar,top3_alarm_num double from "expert-system-pg".searching.alarm_hz_hour
   # --      where top2_alarm_name is not null and top3_alarm_name is not null
   #   """

    print(insert_sql)

    cur = conn.cursor()
    cur.execute(insert_sql)
    rows = cur.fetchall()
    df = pd.DataFrame(rows)
    print(df)
