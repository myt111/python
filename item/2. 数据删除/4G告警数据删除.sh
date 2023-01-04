
#!/bin/bash

if [ -n "$1" ] ;then
   do_date=$1
else
   do_date=`date -d "-7 day" "+%Y%m%d"`
fi

logdir=/bigdata/data_warehouse/zhengzhou/3_MR/4G/${do_date}


function cleandata(){
  # 删除超过6天的日志文件
find ${logdir}  -type f  -delete

rm -fr ${logdir}
}

cleandata


#rm -fr ${logdir}

#find /bigdata/data_warehouse/zhengzhou/3_MR/4G/20221121 -type f   -delete
