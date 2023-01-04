from  clickhouse_sqlalchemy import make_session
from sqlalchemy import create_engine
import pandas as pd

def conn():
    conf = {
        "user": "default",
        "password": "YJY_cu#unicom502",
        "server_host": "10.1.77.51",
        "port": "8123",
        "db": "default"
    }

    connection = 'clickhouse://{user}:{password}@{server_host}:{port}/{db}'.format(**conf)
    engine = create_engine(connection, pool_size=100, pool_recycle=3600, pool_timeout=20)

    sql = 'select  * from default.alarm_hz_day limit 10'

    session = make_session(engine)
    cursor = session.execute(sql)

    data =  pd.DataFrame(cursor)


    print(data)

    cursor.close()
    session.close()

    return data

if __name__ == '__main__':
    conn()
    print("suuccess")