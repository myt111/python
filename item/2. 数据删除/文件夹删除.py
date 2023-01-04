import datetime as dt
import shutil

# 复制、移动、删除、压缩、解压等操作
def get_date():
    today = dt.datetime.today()
    day = today + dt.timedelta(days=(-14))

    day = day.strftime('%Y%m%d')
   # yestoday_time_extra = yestoday.strftime('%Y%m%d')
    return day

if __name__=="__main__":
    path = r'/bigdata/data_warehouse/zhengzhou/2_PM/4G'
    day = get_date()
    data_path = path+"/"+day
    shutil.rmtree(data_path)
    print("{}日pm数据删除完毕".format(day))