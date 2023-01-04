import datetime as dt

if __name__ == '__main__':

    today = dt.datetime.today()
    hour = today + dt.timedelta(hours=(-1))

    hour_daytime = hour.strftime('%Y-%m-%d-%H')
    yestoday_time_extra = hour.strftime('%Y%m%d%H')

    print("test111")


    print("ok")