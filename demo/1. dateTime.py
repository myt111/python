import datetime as dt


def get_time_list(days_num):
    now_time = dt.datetime.now()
    date_list = []
    count=0
    while count< (-days_num):
        temp_date = now_time.strftime('%Y%m%d')
        if count==0:
            max_date=temp_date
        date_list.append(temp_date)
        now_time = now_time + dt.timedelta(days=-1)
        min_date = temp_date
        count=count+1

    date_part=[int(min_date),int(max_date)]
    print(date_part)
    print(date_list)
    return date_list,date_part

def get_date():
    today  = dt.datetime.today()
    yestoday = today + dt.timedelta(days=-1)
    #today_time = today.strftime('%Y%m%d')
    yestoday_date = yestoday.strftime('%Y%m%d')
    today_date = today.strftime('%Y%m%d')
    return yestoday_date ,today_date

if __name__ == '__main__':
    days_num = -7
    # 获取七天内的日期
    date_list, date_part =  get_time_list(days_num)
    for i in date_part:
        print("date_part: "+ str(i))
    for j in date_list:
        print("date_list: "+ str(j))

    # 获取昨天和今天的日期
    yestoday,today = get_date()
    print("yestoday: " + yestoday)
    print("today: " + today)

    print("test")



