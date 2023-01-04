import trino
from trino import transaction
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from io import StringIO



def postgre_conn():
    conn = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502",
                            host="10.1.77.51", port="5432")
    print("Postgres Connection success")
    return conn


def postgre_execute(conn, sql):
    cursor = conn.cursor()
    cursor.execute(sql)
    # data = cursor.fetchmany(size = 3)
    data = cursor.fetchall()
    if len(data) == 0:
        pass
    else:
        data = pd.DataFrame(data)
        des = cursor.description
        colname = []
        for item in des:
            colname.append(item[0])
        data.columns = colname

        print("Postgre Execute success")
        cursor.close()
        conn.close()

    return data

def trino_expert(data):

    engine = create_engine('trino://root@trino.cnio.local:80/expert-pg/')

    data.to_sql("siteinfo",engine,schema="public", index=False, if_exists='append')


    # result = data
    # output = StringIO()
    # result.to_csv(output, sep='\t', index=False, header=False)
    # output1 = output.getvalue()
    # conn = psycopg2.connect(host="trino.cnio.local", user="root", password="YJY_exp#exp502",
    #                         database="hive", port="5432", options="-c search_path=cnio")
    # cur = conn.cursor()
    # cur.copy_from(StringIO(output1), "trace_snbcell_ho", null="")
    #
    # print("trace_ho_snbcell sync：success")
    # conn.commit()
    #
    # cur.close()
    # conn.close()

# '0E-24' is not a valid decimal literal

if __name__ == '__main__':
    conn = postgre_conn()
    sql_hour = ''' select * from "expert-system".cnio."siteinfo" '''
    data = postgre_execute(conn,sql_hour)
    trino_expert(data)
    print("success")

