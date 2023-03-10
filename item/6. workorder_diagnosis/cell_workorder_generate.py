import pandas as pd
import psycopg2
import numpy as np
from sqlalchemy import create_engine
import datetime
from sqlalchemy import create_engine
import multiprocessing as mp
import io
import math


def postgre_expert(data, tablename, schemaname):
    engine = create_engine('postgresql://expert-system:YJY_exp#exp502@133.160.191.111:5432/expert-system')
    #engine = create_engine('postgresql://expert-system:YJY_exp#exp502@10.1.77.51:5432/expert-system')
    data.to_sql(tablename, engine, schema=schemaname, index=False, if_exists='append',method='multi')


def date_task():
    #taskid = {"taskid": [6, 8], "file_filter": ['20220919', '20220920']}
    # taskid = {"taskid": [1, 3, 4, 5,6,7,9], "file_filter": ['20220915','20220916','20220917','20220918',
    #                                                              '20220919','20220921','20220920']}
    taskid = {"taskid": [1, 3, 4, 5,6,7], "file_filter": ['20220915','20220916','20220917','20220918',
                                                                 '20220919','20220921']}
    taskid = pd.DataFrame(taskid)
    return taskid


# def data_insert(data):
#     conn = psycopg2.connect(database="expert-system", user="expert-system", password="YJY_exp#exp502",
#                             host="10.1.77.51", port="5432")
#
#     print("Postgres Connection success")
#     cur = conn.cursor()
#     for i in range(data.shape[0]):
#         try:
#             sql_insert = "insert into cnio.fault_workorder ( order_id, order_code, fault_catalog," \
#                          "fault_class, fault_subclass,cell_name, lon, lat, rel_id, tuning_action, root_cause," \
#                          "order_status,order_date, create_time, rel_id2, comments, province, city," \
#                          "date_on_list, expert_analysis, field_test, order_sorting," \
#                          "antenna_feeder_type) values" \
#                          " (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
#             list = data.loc[i, :]
#             list = list.to_list()
#             list[0] = int(list[0])
#             list[8] = int(list[8])
#             list[19] = int(list[19])
#             list[20] = int(list[20])
#             cur.execute(sql_insert, list)
#             cur.commit()
#         except:
#             print(list)
#     cur.close()
#     conn.close()
#
#     print("Postgres Connection release")


def postgre_antenna_reverse(threshhold_day):
    ##????????????
    # date_list = data_date_list()
    ##??????????????????
    # conn = psycopg2.connect(database="trace-tables", user="trace-tables", password="YJY_tra#tra502", host="10.1.77.51",
    #                         port="5432")
    print("Postgres Connection success")
    conn = psycopg2.connect(database="sk_data", user="sk", password="!@#BrrDztg==", host="133.160.191.108", port="5432")
    # print("Postgres Connection success")
    cur = conn.cursor()
    # ????????????
    # sql_trace_sk_task  = """
    # select taskid,file_filter from public.sk_task where (file_filter =%s and status='??????')  or   (file_filter =%s and status='??????') or
    # (file_filter =%s and status='??????') or (file_filter =%s and status='??????') or (file_filter =%s and status='??????') or
    # (file_filter =%s and status='??????') or (file_filter =%s and status='??????')
    # """
    # # ??????taskid
    # cur.execute(sql_trace_sk_task, date_list)
    # taskid  = cur.fetchall()

    ###
    taskid = date_task()
    if len(taskid) != 0:
        taskid = pd.DataFrame(taskid)
        # taskid.columns = ["taskid","file_filter"]
        for i in range(taskid.shape[0]):
            temp_sql = "(select cellname,eci,celllon,celllat from public.task_" + str(taskid.loc[i, "taskid"]) + \
                       "_lte_cell_sum where azimuthcheck=3 and indoor =0) "
            cur.execute(temp_sql)
            antenna_reverse = cur.fetchall()
            antenna_reverse = pd.DataFrame(antenna_reverse)
            # ?????????
            des = cur.description
            # ????????????
            colname = []
            for item in des:
                colname.append(item[0])
            antenna_reverse.columns = colname
            antenna_reverse["trace_date"] = taskid.loc[i, "file_filter"]

            if i == 0:
                antenna_reverse_data = antenna_reverse
            else:
                antenna_reverse_data = pd.concat([antenna_reverse_data, antenna_reverse])
        antenna_reverse_data.index = range(antenna_reverse_data.shape[0])
        ##??????????????????
        antenna_r_d = antenna_reverse_data.groupby([antenna_reverse_data["cellname"],
                                                    antenna_reverse_data["eci"], antenna_reverse_data["celllon"],
                                                    antenna_reverse_data["celllat"]])["trace_date"].count()
        antenna_r_d = antenna_r_d.reset_index()
        antenna_r_d = antenna_r_d[antenna_r_d["trace_date"] >= threshhold_day]  ###??????????????????4
        antenna_r_d["date_on_list"] = ""
        antenna_r_d.index = range(antenna_r_d.shape[0])
        for i in range(antenna_r_d.shape[0]):
            for j in range(antenna_reverse_data.shape[0]):
                if antenna_r_d.loc[i, "eci"] == antenna_reverse_data.loc[j, "eci"]:
                    if antenna_r_d.loc[i, "date_on_list"] == "":
                        antenna_r_d.loc[i, "date_on_list"] = antenna_reverse_data.loc[j, "trace_date"] + "=1"
                    else:
                        antenna_r_d.loc[i, "date_on_list"] = antenna_r_d.loc[i, "date_on_list"] + "," + \
                                                             antenna_reverse_data.loc[j, "trace_date"] + "=1"
        for i in range(antenna_r_d.shape[0]):
            for j in range(taskid.shape[0]):
                if taskid.loc[j, "file_filter"] not in antenna_r_d.loc[i, "date_on_list"]:
                    antenna_r_d.loc[i, "date_on_list"] = antenna_r_d.loc[i, "date_on_list"] + "," + taskid.loc[
                        j, "file_filter"] + "=0"
        antenna_r_d.rename(columns={"eci": "rel_id", "celllon": "lon", "celllat": "lat", "cellname": "cell_name"},
                           inplace=True)
        ##????????????
        antenna_r_d["order_id"] = antenna_r_d.index
        antenna_r_d["order_code"] = None
        antenna_r_d["fault_catalog"] = "?????????"
        antenna_r_d["fault_class"] = "????????????"
        antenna_r_d["fault_subclass"] = "????????????????????????"
        antenna_r_d["tuning_action"] = "????????????"
        antenna_r_d["root_cause"] = "??????????????????"
        antenna_r_d["order_status"] = ""
        min_date = min(antenna_reverse_data["trace_date"])
        antenna_r_d["order_date"] = datetime.date(int(min_date[:4]), int(min_date[4:6]), int(min_date[6:8]))
        antenna_r_d["create_time"] = datetime.datetime.now()
        antenna_r_d["rel_id2"] = None
        antenna_r_d["expert_analysis"] = 0
        antenna_r_d["field_test"] = 0
        antenna_r_d["comments"] = ""
        antenna_r_d["province"] = "??????"
        antenna_r_d["city"] = "??????"
        antenna_r_d["order_sorting"] = "??????"
        antenna_r_d["antenna_feeder_type"] = ""

        ##????????????
        antenna_r_d = antenna_r_d[["order_id", "order_code", "fault_catalog", "fault_class", "fault_subclass",
                                   "cell_name", "lon", "lat", "rel_id", "tuning_action", "root_cause",
                                   "order_status",
                                   "order_date", "create_time", "rel_id2", "comments", "province", "city",
                                   "date_on_list", "expert_analysis", "field_test", "order_sorting",
                                   "antenna_feeder_type"]]

        postgre_expert(antenna_r_d, "fault_workorder_r2", "cnio")

        print("antena reverse workorder is inseted into pgsql successful")

    cur.close()
    conn.close()

    print("Postgres Connection release")
    return antenna_r_d


def postgre_atenna_ab_lobe(threshhold_day):
    ##??????????????????
    # date_list = data_date_list()
    ##??????????????????
    # conn = psycopg2.connect(database="trace-tables", user="trace-tables", password="YJY_tra#tra502", host="10.1.77.51", port="5432")
    conn = psycopg2.connect(database="sk_data", user="sk", password="!@#BrrDztg==", host="133.160.191.108", port="5432")

    print("Postgres Connection success")
    cur = conn.cursor()
    # ????????????
    # sql_trace_sk_task  = """
    # select taskid,file_filter from public.sk_task where (file_filter =%s and status='??????')  or   (file_filter =%s and status='??????') or
    # (file_filter =%s and status='??????') or (file_filter =%s and status='??????') or (file_filter =%s and status='??????') or
    # (file_filter =%s and status='??????') or (file_filter =%s and status='??????')
    # """
    # # ??????taskid
    # cur.execute(sql_trace_sk_task, date_list)
    taskid = date_task()
    # taskid  = cur.fetchall()
    if len(taskid) != 0:
        taskid = pd.DataFrame(taskid)
        for i in range(taskid.shape[0]):
            ## ?????? 7???????????????
            temp_sql = "(select cellname,eci,celllon,celllat from public.task_" + str(taskid.loc[i, "taskid"]) + \
                       "_lte_cell_sum where azimuthcheck=1) "
            cur.execute(temp_sql)
            antenna_ab = cur.fetchall()
            antenna_ab = pd.DataFrame(antenna_ab)
            # ?????????
            des = cur.description
            # ????????????
            colname = []
            for item in des:
                colname.append(item[0])
            antenna_ab.columns = colname
            antenna_ab["trace_date"] = taskid.loc[i, "file_filter"]

            if i == 0:
                antenna_ab_data = antenna_ab
            else:
                antenna_ab_data = pd.concat([antenna_ab_data, antenna_ab])
        antenna_ab_data.index = range(antenna_ab_data.shape[0])
        ##??????????????????
        antenna_ab_list = antenna_ab_data.groupby([antenna_ab_data["cellname"],
                                                   antenna_ab_data["eci"], antenna_ab_data["celllon"],
                                                   antenna_ab_data["celllat"]]).count()
        antenna_ab_list = antenna_ab_list.reset_index()
        antenna_ab_list = antenna_ab_list[antenna_ab_list["trace_date"] >= threshhold_day]
        antenna_ab_list["date_on_list"] = ""
        antenna_ab_list.index = range(antenna_ab_list.shape[0])

        for i in range(antenna_ab_list.shape[0]):
            for j in range(antenna_ab_data.shape[0]):
                if antenna_ab_list.loc[i, "eci"] == antenna_ab_data.loc[j, "eci"]:
                    if antenna_ab_list.loc[i, "date_on_list"] == "":
                        antenna_ab_list.loc[i, "date_on_list"] = antenna_ab_data.loc[j, "trace_date"] + "=1"
                    else:
                        antenna_ab_list.loc[i, "date_on_list"] = antenna_ab_list.loc[i, "date_on_list"] + "," + \
                                                             antenna_ab_data.loc[j, "trace_date"] + "=1"

        for i in range(antenna_ab_list.shape[0]):
            for j in range(taskid.shape[0]):
                if taskid.loc[j, "file_filter"] not in antenna_ab_list.loc[i, "date_on_list"]:
                    antenna_ab_list.loc[i, "date_on_list"] = antenna_ab_list.loc[i, "date_on_list"] + "," + taskid.loc[
                        j, "file_filter"] + "=0"
        antenna_ab_list.rename(columns={"eci": "rel_id", "celllon": "lon", "celllat": "lat", "cellname": "cell_name"},
                               inplace=True)
        ##????????????
        antenna_ab_list["order_id"] = antenna_ab_list.index
        antenna_ab_list["order_code"] = None
        antenna_ab_list["fault_catalog"] = "?????????"
        antenna_ab_list["fault_class"] = "????????????"
        antenna_ab_list["fault_subclass"] = "?????????????????????????????????"
        antenna_ab_list["tuning_action"] = "????????????"
        antenna_ab_list["root_cause"] = "?????????????????????"
        antenna_ab_list["order_status"] = ""
        min_date = min(antenna_ab_data["trace_date"])
        antenna_ab_list["order_date"] = datetime.date(int(min_date[:4]), int(min_date[4:6]), int(min_date[6:8]))
        antenna_ab_list["create_time"] = datetime.datetime.now()
        antenna_ab_list["rel_id2"] = None
        antenna_ab_list["expert_analysis"] = 0
        antenna_ab_list["field_test"] = 0
        antenna_ab_list["comments"] = ""
        antenna_ab_list["province"] = "??????"
        antenna_ab_list["city"] = "??????"
        antenna_ab_list["order_sorting"] = "??????"
        antenna_ab_list["antenna_feeder_type"] = ""
        ##????????????
        antenna_ab_list = antenna_ab_list[["order_id", "order_code", "fault_catalog", "fault_class", "fault_subclass",
                                           "cell_name", "lon", "lat", "rel_id", "tuning_action", "root_cause",
                                           "order_status",
                                           "order_date", "create_time", "rel_id2", "comments", "province", "city",
                                           "date_on_list", "expert_analysis", "field_test", "order_sorting",
                                           "antenna_feeder_type"]]
        postgre_expert(antenna_ab_list, "fault_workorder_r2", "cnio")
        print("antena abnormal lobe  workorder is inseted into pgsql successful")

    cur.close()
    conn.close()

    print("Postgres Connection release")


def postgre_atenna_azimuth_error(threshhold_day):
    ##????????????
    # date_list = data_date_list()

    ##??????????????????
    # conn = psycopg2.connect(database="trace-tables", user="trace-tables", password="YJY_tra#tra502", host="10.1.77.51", port="5432")
    conn = psycopg2.connect(database="sk_data", user="sk", password="!@#BrrDztg==", host="133.160.191.108", port="5432")

    print("Postgres Connection success")
    cur = conn.cursor()
    # ????????????
    # sql_trace_sk_task  = """
    # select taskid,file_filter from public.sk_task where (file_filter =%s and status='??????')  or   (file_filter =%s and status='??????') or
    # (file_filter =%s and status='??????') or (file_filter =%s and status='??????') or (file_filter =%s and status='??????') or
    # (file_filter =%s and status='??????') or (file_filter =%s and status='??????')
    # """
    # # ??????taskid
    # cur.execute(sql_trace_sk_task, date_list)
    # taskid  = cur.fetchall()
    taskid = date_task()
    if len(taskid) != 0:
        taskid = pd.DataFrame(taskid)
        taskid.columns = ["taskid", "file_filter"]
        temp_sql = ""
        for i in range(taskid.shape[0]):
            ## ?????? 7???????????????
            temp_sql = "(select cellname,eci,celllon,celllat from public.task_" + str(taskid.loc[i, "taskid"]) + \
                       "_lte_cell_sum where azimuthcheck=2) "
            cur.execute(temp_sql)
            antenna_ab = cur.fetchall()
            antenna_ab = pd.DataFrame(antenna_ab)
            # ?????????
            des = cur.description
            # ????????????
            colname = []
            for item in des:
                colname.append(item[0])
            antenna_ab.columns = colname
            antenna_ab["trace_date"] = taskid.loc[i, "file_filter"]

            if i == 0:
                antenna_ab_data = antenna_ab
            else:
                antenna_ab_data = pd.concat([antenna_ab_data, antenna_ab])
        antenna_ab_data.index = range(antenna_ab_data.shape[0])
        ##??????????????????
        antenna_ab_list = antenna_ab_data.groupby([antenna_ab_data["cellname"],
                                                   antenna_ab_data["eci"], antenna_ab_data["celllon"],
                                                   antenna_ab_data["celllat"]]).count()
        antenna_ab_list = antenna_ab_list.reset_index()
        antenna_ab_list = antenna_ab_list[antenna_ab_list["trace_date"] >= threshhold_day]
        antenna_ab_list["date_on_list"] = ""
        antenna_ab_list.index = range(antenna_ab_list.shape[0])

        ## ????????????
        for i in range(antenna_ab_list.shape[0]):
            for j in range(antenna_ab_data.shape[0]):
                if antenna_ab_list.loc[i, "eci"] == antenna_ab_data.loc[j, "eci"]:
                    if antenna_ab_list.loc[i, "date_on_list"] == "":
                        antenna_ab_list.loc[i, "date_on_list"] = antenna_ab_data.loc[j, "trace_date"] + "=1"
                    else:
                        antenna_ab_list.loc[i, "date_on_list"] = antenna_ab_list.loc[i, "date_on_list"] + "," + \
                                                             antenna_ab_data.loc[j, "trace_date"] + "=1"
        ## ??????????????????
        for i in range(antenna_ab_list.shape[0]):
            for j in range(taskid.shape[0]):
                if taskid.loc[j, "file_filter"] not in antenna_ab_list.loc[i, "date_on_list"]:
                    antenna_ab_list.loc[i, "date_on_list"] = antenna_ab_list.loc[i, "date_on_list"] + "," + taskid.loc[
                        j, "file_filter"] + "=0"
        antenna_ab_list.rename(columns={"eci": "rel_id", "celllon": "lon", "celllat": "lat", "cellname": "cell_name"},
                               inplace=True)
        ##????????????
        antenna_ab_list["order_id"] = antenna_ab_list.index
        antenna_ab_list["order_code"] = None
        antenna_ab_list["fault_catalog"] = "?????????"
        antenna_ab_list["fault_class"] = "????????????"
        antenna_ab_list["fault_subclass"] = "?????????????????????????????????"
        antenna_ab_list["tuning_action"] = "????????????"
        antenna_ab_list["root_cause"] = "???????????????????????????"
        antenna_ab_list["order_status"] = ""
        min_date = min(antenna_ab_data["trace_date"])
        antenna_ab_list["order_date"] = datetime.date(int(min_date[:4]), int(min_date[4:6]), int(min_date[6:8]))
        antenna_ab_list["create_time"] = datetime.datetime.now()
        antenna_ab_list["rel_id2"] = None
        antenna_ab_list["expert_analysis"] = 0
        antenna_ab_list["field_test"] = 0
        antenna_ab_list["comments"] = ""
        antenna_ab_list["province"] = "??????"
        antenna_ab_list["city"] = "??????"
        antenna_ab_list["order_sorting"] = "??????"
        antenna_ab_list["antenna_feeder_type"] = ""
        ##????????????
        antenna_ab_list = antenna_ab_list[["order_id", "order_code", "fault_catalog", "fault_class", "fault_subclass",
                                           "cell_name", "lon", "lat", "rel_id", "tuning_action", "root_cause",
                                           "order_status",
                                           "order_date", "create_time", "rel_id2", "comments", "province", "city",
                                           "date_on_list", "expert_analysis", "field_test", "order_sorting",
                                           "antenna_feeder_type"]]
        postgre_expert(antenna_ab_list, "fault_workorder_r2", "cnio")
        print("antena azimuth error  workorder is inseted into pgsql successful")
    cur.close()
    conn.close()

    print("Postgres Connection release")


def postgre_cell_location_error(threshhold_day):
    ##????????????
    # date_list = data_date_list()
    ##??????????????????
    # conn = psycopg2.connect(database="trace-tables", user="trace-tables", password="YJY_tra#tra502", host="10.1.77.51", port="5432")
    conn = psycopg2.connect(database="sk_data", user="sk", password="!@#BrrDztg==", host="133.160.191.108", port="5432")

    print("Postgres Connection success")
    cur = conn.cursor()
    # ????????????
    # sql_trace_sk_task  = """
    # select taskid,file_filter from public.sk_task where (file_filter =%s and status='??????')  or   (file_filter =%s and status='??????') or
    # (file_filter =%s and status='??????') or (file_filter =%s and status='??????') or (file_filter =%s and status='??????') or
    # (file_filter =%s and status='??????') or (file_filter =%s and status='??????')
    # """
    # # ??????taskid
    # cur.execute(sql_trace_sk_task, date_list)
    # taskid  = cur.fetchall()
    taskid = date_task()
    if len(taskid) != 0:
        taskid = pd.DataFrame(taskid)
        taskid.columns = ["taskid", "file_filter"]
        temp_sql = ""
        for i in range(taskid.shape[0]):
            ## ?????? 7???????????????
            temp_sql = "(select cellname,eci,celllon,celllat from public.task_" + str(taskid.loc[i, "taskid"]) + \
                       "_lte_cell_sum where locationerr=1) "
            cur.execute(temp_sql)
            cell_ab = cur.fetchall()
            cell_ab = pd.DataFrame(cell_ab)
            # ?????????
            des = cur.description
            # ????????????
            colname = []
            for item in des:
                colname.append(item[0])
            cell_ab.columns = colname
            cell_ab["trace_date"] = taskid.loc[i, "file_filter"]

            if i == 0:
                cell_ab_data = cell_ab
            else:
                cell_ab_data = pd.concat([cell_ab_data, cell_ab])
        cell_ab_data.index = range(cell_ab_data.shape[0])
        ##??????????????????
        cell_ab_list = cell_ab_data.groupby([cell_ab_data["cellname"],
                                             cell_ab_data["eci"], cell_ab_data["celllon"],
                                             cell_ab_data["celllat"]]).count()
        cell_ab_list = cell_ab_list.reset_index()
        cell_ab_list = cell_ab_list[cell_ab_list["trace_date"] >= threshhold_day]
        cell_ab_list["date_on_list"] = ""
        cell_ab_list.index = range(cell_ab_list.shape[0])


        for i in range(cell_ab_list.shape[0]):
            for j in range(cell_ab_data.shape[0]):
                if cell_ab_list.loc[i, "eci"] == cell_ab_data.loc[j, "eci"]:
                    if cell_ab_list.loc[i, "date_on_list"] == "":
                        cell_ab_list.loc[i, "date_on_list"] = cell_ab_data.loc[j, "trace_date"] + "=1"
                    else:
                        cell_ab_list.loc[i, "date_on_list"] = cell_ab_list.loc[i, "date_on_list"] + "," + \
                                                             cell_ab_data.loc[j, "trace_date"] + "=1"


        for i in range(cell_ab_list.shape[0]):
            for j in range(taskid.shape[0]):
                if taskid.loc[j, "file_filter"] not in cell_ab_list.loc[i, "date_on_list"]:
                    cell_ab_list.loc[i, "date_on_list"] = cell_ab_list.loc[i, "date_on_list"] + "," + taskid.loc[
                        j, "file_filter"] + "=0"
        cell_ab_list.rename(columns={"eci": "rel_id", "celllon": "lon", "celllat": "lat", "cellname": "cell_name"},
                            inplace=True)
        ##????????????
        cell_ab_list["order_id"] = cell_ab_list.index
        cell_ab_list["order_code"] = None
        cell_ab_list["fault_catalog"] = "?????????"
        cell_ab_list["fault_class"] = "????????????"
        cell_ab_list["fault_subclass"] = "????????????????????????"
        cell_ab_list["tuning_action"] = "??????????????????"
        cell_ab_list["root_cause"] = "????????????????????????"
        cell_ab_list["order_status"] = ""
        min_date = min(cell_ab_data["trace_date"])
        cell_ab_list["order_date"] = datetime.date(int(min_date[:4]), int(min_date[4:6]), int(min_date[6:8]))
        cell_ab_list["create_time"] = datetime.datetime.now()
        cell_ab_list["rel_id2"] = None
        cell_ab_list["expert_analysis"] = 0
        cell_ab_list["field_test"] = 0
        cell_ab_list["comments"] = ""
        cell_ab_list["province"] = "??????"
        cell_ab_list["city"] = "??????"
        cell_ab_list["order_sorting"] = "??????"
        cell_ab_list["antenna_feeder_type"] = ""
        ##????????????
        cell_ab_list = cell_ab_list[["order_id", "order_code", "fault_catalog", "fault_class", "fault_subclass",
                                     "cell_name", "lon", "lat", "rel_id", "tuning_action", "root_cause", "order_status",
                                     "order_date", "create_time", "rel_id2", "comments", "province", "city",
                                     "date_on_list", "expert_analysis", "field_test", "order_sorting",
                                     "antenna_feeder_type"]]
        postgre_expert(cell_ab_list, "fault_workorder_r2", "cnio")
        print("cell location error  workorder is inseted into pgsql successful")
    cur.close()
    conn.close()

    print("Postgres Connection release")


def postgre_indoorleak(threshhold_day):
    ##????????????
    # date_list = data_date_list()
    ##??????????????????
    # conn = psycopg2.connect(database="trace-tables", user="trace-tables", password="YJY_tra#tra502", host="10.1.77.51", port="5432")
    conn = psycopg2.connect(database="sk_data", user="sk", password="!@#BrrDztg==", host="133.160.191.108", port="5432")

    print("Postgres Connection success")
    cur = conn.cursor()
    # ????????????
    # sql_trace_sk_task  = """
    # select taskid,file_filter from public.sk_task where (file_filter =%s and status='??????')  or   (file_filter =%s and status='??????') or
    # (file_filter =%s and status='??????') or (file_filter =%s and status='??????') or (file_filter =%s and status='??????') or
    # (file_filter =%s and status='??????') or (file_filter =%s and status='??????')
    # """
    # # ??????taskid
    # cur.execute(sql_trace_sk_task, date_list)
    # taskid  = cur.fetchall()
    taskid = date_task()
    if len(taskid) != 0:
        taskid = pd.DataFrame(taskid)
        taskid.columns = ["taskid", "file_filter"]
        temp_sql = ""
        for i in range(taskid.shape[0]):
            ## ?????? 7???????????????
            temp_sql = "(select cellname,eci,celllon,celllat from public.task_" + str(taskid.loc[i, "taskid"]) + \
                       "_lte_cell_sum where indoorleak=1) "
            cur.execute(temp_sql)
            cell_ab = cur.fetchall()
            cell_ab = pd.DataFrame(cell_ab)
            # ?????????
            des = cur.description
            # ????????????
            colname = []
            for item in des:
                colname.append(item[0])
            cell_ab.columns = colname
            cell_ab["trace_date"] = taskid.loc[i, "file_filter"]

            if i == 0:
                cell_ab_data = cell_ab
            else:
                cell_ab_data = pd.concat([cell_ab_data, cell_ab])
        cell_ab_data.index = range(cell_ab_data.shape[0])
        ##??????????????????
        cell_ab_list = cell_ab_data.groupby([cell_ab_data["cellname"],
                                             cell_ab_data["eci"], cell_ab_data["celllon"],
                                             cell_ab_data["celllat"]]).count()
        cell_ab_list = cell_ab_list.reset_index()

        cell_ab_list = cell_ab_list[cell_ab_list["trace_date"] >= threshhold_day]
        cell_ab_list["date_on_list"] = ""
        cell_ab_list.index = range(cell_ab_list.shape[0])

        for i in range(cell_ab_list.shape[0]):
            for j in range(cell_ab_data.shape[0]):
                if cell_ab_list.loc[i, "eci"] == cell_ab_data.loc[j, "eci"]:
                    if cell_ab_list.loc[i, "date_on_list"] == "":
                        cell_ab_list.loc[i, "date_on_list"] = cell_ab_data.loc[j, "trace_date"] + "=1"
                    else:
                        cell_ab_list.loc[i, "date_on_list"] = cell_ab_list.loc[i, "date_on_list"] + "," + \
                                                              cell_ab_data.loc[j, "trace_date"] + "=1"

        for i in range(cell_ab_list.shape[0]):
            for j in range(taskid.shape[0]):
                if taskid.loc[j, "file_filter"] not in cell_ab_list.loc[i, "date_on_list"]:
                    cell_ab_list.loc[i, "date_on_list"] = cell_ab_list.loc[i, "date_on_list"] + "," + taskid.loc[
                        j, "file_filter"] + "=0"

        cell_ab_list.rename(columns={"eci": "rel_id", "celllon": "lon", "celllat": "lat", "cellname": "cell_name"},
                            inplace=True)
        ##????????????
        cell_ab_list["order_id"] = cell_ab_list.index
        cell_ab_list["order_code"] = None
        cell_ab_list["fault_catalog"] = "?????????"
        cell_ab_list["fault_class"] = "????????????"
        cell_ab_list["fault_subclass"] = "????????????????????????"
        cell_ab_list["tuning_action"] = "??????????????????????????????"
        cell_ab_list["root_cause"] = "????????????"
        cell_ab_list["order_status"] = ""
        min_date = min(cell_ab_data["trace_date"])
        cell_ab_list["order_date"] = datetime.date(int(min_date[:4]), int(min_date[4:6]), int(min_date[6:8]))
        cell_ab_list["create_time"] = datetime.datetime.now()
        cell_ab_list["rel_id2"] = None
        cell_ab_list["expert_analysis"] = 0
        cell_ab_list["field_test"] = 0
        cell_ab_list["comments"] = ""
        cell_ab_list["province"] = "??????"
        cell_ab_list["city"] = "??????"
        cell_ab_list["order_sorting"] = "??????"
        cell_ab_list["antenna_feeder_type"] = ""
        ##????????????
        cell_ab_list = cell_ab_list[["order_id", "order_code", "fault_catalog", "fault_class", "fault_subclass",
                                     "cell_name", "lon", "lat", "rel_id", "tuning_action", "root_cause", "order_status",
                                     "order_date", "create_time", "rel_id2", "comments", "province", "city",
                                     "date_on_list", "expert_analysis", "field_test", "order_sorting",
                                     "antenna_feeder_type"]]
        postgre_expert(cell_ab_list, "fault_workorder_r2", "cnio")
        print("indoorleak  workorder is inseted into pgsql successful")
    cur.close()
    conn.close()

    print("Postgres Connection release")


def postgre_overshooting(threshhold_day):
    ##????????????
    # date_list = data_date_list()

    ##??????????????????
    # conn = psycopg2.connect(database="trace-tables", user="trace-tables", password="YJY_tra#tra502", host="10.1.77.51", port="5432")
    conn = psycopg2.connect(database="sk_data", user="sk", password="!@#BrrDztg==", host="133.160.191.108", port="5432")

    print("Postgres Connection success")
    cur = conn.cursor()
    # ????????????
    # sql_trace_sk_task  = """
    # select taskid,file_filter from public.sk_task where (file_filter =%s and status='??????')  or   (file_filter =%s and status='??????') or
    # (file_filter =%s and status='??????') or (file_filter =%s and status='??????') or (file_filter =%s and status='??????') or
    # (file_filter =%s and status='??????') or (file_filter =%s and status='??????')
    # """
    # # ??????taskid
    # cur.execute(sql_trace_sk_task, date_list)
    # taskid  = cur.fetchall()
    taskid = date_task()
    if len(taskid) != 0:
        taskid = pd.DataFrame(taskid)
        taskid.columns = ["taskid", "file_filter"]
        temp_sql = ""

        ###SQL ????????????????????????
        for i in range(taskid.shape[0]):
            ## ?????? 7???????????????
            temp_sql = "(select cellname,eci,celllon,celllat from public.task_" + str(taskid.loc[i, "taskid"]) + \
                       "_lte_cell_sum where overshooting=1) "
            cur.execute(temp_sql)
            cell_ab = cur.fetchall()
            cell_ab = pd.DataFrame(cell_ab)
            # ?????????
            des = cur.description
            # ????????????
            colname = []
            for item in des:
                colname.append(item[0])
            cell_ab.columns = colname
            cell_ab["trace_date"] = taskid.loc[i, "file_filter"]

            if i == 0:
                cell_ab_data = cell_ab
            else:
                cell_ab_data = pd.concat([cell_ab_data, cell_ab])
        cell_ab_data.index = range(cell_ab_data.shape[0])
        ##??????????????????
        cell_ab_list = cell_ab_data.groupby([cell_ab_data["cellname"],
                                             cell_ab_data["eci"], cell_ab_data["celllon"],
                                             cell_ab_data["celllat"]]).count()
        cell_ab_list = cell_ab_list.reset_index()
        cell_ab_list = cell_ab_list[cell_ab_list["trace_date"] >= threshhold_day]
        cell_ab_list["date_on_list"] = ""
        cell_ab_list.index = range(cell_ab_list.shape[0])


        for i in range(cell_ab_list.shape[0]):
            for j in range(cell_ab_data.shape[0]):
                if cell_ab_list.loc[i, "eci"] == cell_ab_data.loc[j, "eci"]:
                    if cell_ab_list.loc[i, "date_on_list"] == "":
                        cell_ab_list.loc[i, "date_on_list"] = cell_ab_data.loc[j, "trace_date"] + "=1"
                    else:
                        cell_ab_list.loc[i, "date_on_list"] = cell_ab_list.loc[i, "date_on_list"] + "," + \
                                                              cell_ab_data.loc[j, "trace_date"] + "=1"

        for i in range(cell_ab_list.shape[0]):
            for j in range(taskid.shape[0]):
                if taskid.loc[j, "file_filter"] not in cell_ab_list.loc[i, "date_on_list"]:
                    cell_ab_list.loc[i, "date_on_list"] = cell_ab_list.loc[i, "date_on_list"] + "," + taskid.loc[
                        j, "file_filter"] + "=0"
        cell_ab_list.rename(columns={"eci": "rel_id", "celllon": "lon", "celllat": "lat", "cellname": "cell_name"},
                            inplace=True)
        ##????????????
        cell_ab_list["order_id"] = cell_ab_list.index
        cell_ab_list["order_code"] = None
        cell_ab_list["fault_catalog"] = "?????????"
        cell_ab_list["fault_class"] = "????????????"
        cell_ab_list["fault_subclass"] = "??????????????????"
        cell_ab_list["tuning_action"] = "?????????????????????????????????????????????"
        cell_ab_list["root_cause"] = "????????????"
        cell_ab_list["order_status"] = ""
        min_date = min(cell_ab_data["trace_date"])
        cell_ab_list["order_date"] = datetime.date(int(min_date[:4]), int(min_date[4:6]), int(min_date[6:8]))
        cell_ab_list["create_time"] = datetime.datetime.now()
        cell_ab_list["rel_id2"] = None
        cell_ab_list["expert_analysis"] = 0
        cell_ab_list["field_test"] = 0
        cell_ab_list["comments"] = ""
        cell_ab_list["province"] = "??????"
        cell_ab_list["city"] = "??????"
        cell_ab_list["order_sorting"] = "??????"
        cell_ab_list["antenna_feeder_type"] = ""
        ##????????????
        cell_ab_list = cell_ab_list[["order_id", "order_code", "fault_catalog", "fault_class", "fault_subclass",
                                     "cell_name", "lon", "lat", "rel_id", "tuning_action", "root_cause", "order_status",
                                     "order_date", "create_time", "rel_id2", "comments", "province", "city",
                                     "date_on_list", "expert_analysis", "field_test", "order_sorting",
                                     "antenna_feeder_type"]]
        postgre_expert(cell_ab_list, "fault_workorder_r2", "cnio")
        print("orvershooting  workorder is inseted into pgsql successful")
    cur.close()
    conn.close()

    print("Postgres Connection release")


def postgre_high_reest(threshhold_day):
    ##????????????
    # date_list = data_date_list()
    ##??????????????????
    # conn = psycopg2.connect(database="trace-tables", user="trace-tables", password="YJY_tra#tra502", host="10.1.77.51", port="5432")
    conn = psycopg2.connect(database="sk_data", user="sk", password="!@#BrrDztg==", host="133.160.191.108", port="5432")

    print("Postgres Connection success")
    cur = conn.cursor()
    # ????????????
    # sql_trace_sk_task  = """
    # select taskid,file_filter from public.sk_task where (file_filter =%s and status='??????')  or   (file_filter =%s and status='??????') or
    # (file_filter =%s and status='??????') or (file_filter =%s and status='??????') or (file_filter =%s and status='??????') or
    # (file_filter =%s and status='??????') or (file_filter =%s and status='??????')
    # """
    # # ??????taskid
    # cur.execute(sql_trace_sk_task, date_list)
    # taskid  = cur.fetchall()
    taskid = date_task()
    if len(taskid) != 0:
        taskid = pd.DataFrame(taskid)
        taskid.columns = ["taskid", "file_filter"]
        temp_sql = ""

        ###SQL ????????????????????????
        for i in range(taskid.shape[0]):
            ## ?????? 7???????????????
            temp_sql = "(select cellname,eci,celllon,celllat from public.task_" + str(taskid.loc[i, "taskid"]) + \
                       "_lte_cell_sum where conntotalnum >100 and reestreqnum*1.0/conntotalnum >0.05) "
            cur.execute(temp_sql)
            cell_ab = cur.fetchall()
            cell_ab = pd.DataFrame(cell_ab)
            # ?????????
            des = cur.description
            # ????????????
            colname = []
            for item in des:
                colname.append(item[0])
            cell_ab.columns = colname
            cell_ab["trace_date"] = taskid.loc[i, "file_filter"]

            if i == 0:
                cell_ab_data = cell_ab
            else:
                cell_ab_data = pd.concat([cell_ab_data, cell_ab])
        cell_ab_data.index = range(cell_ab_data.shape[0])
        ##??????????????????
        cell_ab_list = cell_ab_data.groupby([cell_ab_data["cellname"],
                                             cell_ab_data["eci"], cell_ab_data["celllon"],
                                             cell_ab_data["celllat"]]).count()
        cell_ab_list = cell_ab_list.reset_index()
        cell_ab_list = cell_ab_list[cell_ab_list["trace_date"] >= threshhold_day]
        cell_ab_list["date_on_list"] = ""
        cell_ab_list.index = range(cell_ab_list.shape[0])

        for i in range(cell_ab_list.shape[0]):
            for j in range(cell_ab_data.shape[0]):
                if cell_ab_list.loc[i, "eci"] == cell_ab_data.loc[j, "eci"]:
                    if cell_ab_list.loc[i, "date_on_list"] == "":
                        cell_ab_list.loc[i, "date_on_list"] = cell_ab_data.loc[j, "trace_date"] + "=1"
                    else:
                        cell_ab_list.loc[i, "date_on_list"] = cell_ab_list.loc[i, "date_on_list"] + "," + \
                                                              cell_ab_data.loc[j, "trace_date"] + "=1"

        for i in range(cell_ab_list.shape[0]):
            for j in range(taskid.shape[0]):
                if taskid.loc[j, "file_filter"] not in cell_ab_list.loc[i, "date_on_list"]:
                    cell_ab_list.loc[i, "date_on_list"] = cell_ab_list.loc[i, "date_on_list"] + "," + taskid.loc[
                        j, "file_filter"] + "=0"
        cell_ab_list.rename(columns={"eci": "rel_id", "celllon": "lon", "celllat": "lat", "cellname": "cell_name"},
                            inplace=True)
        ##????????????
        cell_ab_list["order_id"] = cell_ab_list.index
        cell_ab_list["order_code"] = None
        cell_ab_list["fault_catalog"] = "?????????"
        cell_ab_list["fault_class"] = "????????????"
        cell_ab_list["fault_subclass"] = "?????????????????????"
        cell_ab_list["tuning_action"] = "????????????+????????????"
        cell_ab_list["root_cause"] = "1??????????????????????????????2?????????????????????"
        cell_ab_list["order_status"] = ""
        min_date = min(cell_ab_data["trace_date"])
        cell_ab_list["order_date"] = datetime.date(int(min_date[:4]), int(min_date[4:6]), int(min_date[6:8]))
        cell_ab_list["create_time"] = datetime.datetime.now()
        cell_ab_list["rel_id2"] = None
        cell_ab_list["expert_analysis"] = 0
        cell_ab_list["field_test"] = 0
        cell_ab_list["comments"] = ""
        cell_ab_list["province"] = "??????"
        cell_ab_list["city"] = "??????"
        cell_ab_list["order_sorting"] = "??????"
        cell_ab_list["antenna_feeder_type"] = ""
        ##????????????
        cell_ab_list = cell_ab_list[["order_id", "order_code", "fault_catalog", "fault_class", "fault_subclass",
                                     "cell_name", "lon", "lat", "rel_id", "tuning_action", "root_cause", "order_status",
                                     "order_date", "create_time", "rel_id2", "comments", "province", "city",
                                     "date_on_list", "expert_analysis", "field_test", "order_sorting",
                                     "antenna_feeder_type"]]
        postgre_expert(cell_ab_list, "fault_workorder_r2", "cnio")
        print("high reest cell workorder is inseted into pgsql successful")
    cur.close()
    conn.close()

    print("Postgres Connection release")


def postgre_ho_fail_pairs(threshhold_day):
    ##????????????
    # date_list = data_date_list()
    # data_list = ['20220915','']  # 0915~0919
    ##??????????????????
    # conn = psycopg2.connect(database="trace-tables", user="trace-tables", password="YJY_tra#tra502", host="10.1.77.51", port="5432")
    conn = psycopg2.connect(database="sk_data", user="sk", password="!@#BrrDztg==", host="133.160.191.108", port="5432")

    print("Postgres Connection success")
    cur = conn.cursor()
    # ????????????
    # sql_trace_sk_task  = """
    # select taskid,file_filter from public.sk_task where (file_filter =%s and status='??????')  or   (file_filter =%s and status='??????') or
    # (file_filter =%s and status='??????') or (file_filter =%s and status='??????') or (file_filter =%s and status='??????') or
    # (file_filter =%s and status='??????') or (file_filter =%s and status='??????')
    # """
    # ??????taskid
    # cur.execute(sql_trace_sk_task, date_list)
    # taskid  = cur.fetchall()
    taskid = date_task()
    if len(taskid) != 0:
        taskid = pd.DataFrame(taskid)
        taskid.columns = ["taskid", "file_filter"]

        ###SQL ????????????????????????
        for i in range(taskid.shape[0]):
            ## ??????????????????
            temp_sql = "select a.serveci,a.desteci,b.cellname,b.lon,b.lat from " + \
                       "(select serveci,desteci from public.task_" + str(taskid.loc[i, "taskid"]) + \
                       "_lte_ho_snbcell where hoouttotalnum>=1000 and hooutsuccnum/ hoouttotalnum <0.9) a left join" + \
                       " (select cellname,lon/100000.0 as lon,lat/100000.0 as lat,cellindex as eci from public.task_" + \
                       str(taskid.loc[i, "taskid"]) + "_siteinfo)  b on a.serveci=b.eci "
            cur.execute(temp_sql)
            cell_ab = cur.fetchall()
            cell_ab = pd.DataFrame(cell_ab)
            # ?????????
            des = cur.description
            # ????????????
            colname = []
            for item in des:
                colname.append(item[0])
            cell_ab.columns = colname
            cell_ab["trace_date"] = taskid.loc[i, "file_filter"]

            if i == 0:
                cell_ab_data = cell_ab
            else:
                cell_ab_data = pd.concat([cell_ab_data, cell_ab])

        cell_ab_data.index = range(cell_ab_data.shape[0])
        ##??????????????????
        cell_ab_list = cell_ab_data.groupby([cell_ab_data["cellname"],
                                             cell_ab_data["serveci"], cell_ab_data["desteci"], cell_ab_data["lon"],
                                             cell_ab_data["lat"]]).count()
        cell_ab_list = cell_ab_list.reset_index()

        ### ?????????????????????????????????????????????
        cell_ab_list = cell_ab_list[cell_ab_list["trace_date"] >= threshhold_day]
        cell_ab_list["date_on_list"] = ""

        if cell_ab_list.shape[0] > 100:
            ## ????????????????????????100????????????????????????????????????????????????????????????100??????????????????
            cell_ab_list = cell_ab_list.sort_values(by="trace_date", ascending=False)
            cell_ab_list = cell_ab_list.head(100)
            cell_ab_list.index = range(cell_ab_list.shape[0])

        for i in range(cell_ab_list.shape[0]):
            for j in range(cell_ab_data.shape[0]):
                if cell_ab_list.loc[i, "serveci"] == cell_ab_data.loc[j, "serveci"] and \
                        cell_ab_list.loc[i, "desteci"] == cell_ab_data.loc[j, "desteci"]:
                    if cell_ab_list.loc[i, "date_on_list"] == "":
                        cell_ab_list.loc[i, "date_on_list"] = cell_ab_data.loc[j, "trace_date"] + "=1"
                    else:
                        cell_ab_list.loc[i, "date_on_list"] = cell_ab_list.loc[i, "date_on_list"] + "," + \
                                                              cell_ab_data.loc[j, "trace_date"] + "=1"
        for i in range(cell_ab_list.shape[0]):
            for j in range(taskid.shape[0]):
                if taskid.loc[j, "file_filter"] not in cell_ab_list.loc[i, "date_on_list"]:
                    cell_ab_list.loc[i, "date_on_list"] = cell_ab_list.loc[i, "date_on_list"] + "," + taskid.loc[
                        j, "file_filter"] + "=0"

        cell_ab_list.rename(columns={"desteci": "rel_id2", "serveci": "rel_id", "cellname": "cell_name"}, inplace=True)
        ##????????????
        cell_ab_list["order_id"] = cell_ab_list.index
        cell_ab_list["order_code"] = None
        cell_ab_list["fault_catalog"] = "?????????"
        cell_ab_list["fault_class"] = "?????????"
        cell_ab_list["fault_subclass"] = "????????????????????????"
        cell_ab_list["tuning_action"] = "????????????+????????????"
        cell_ab_list["root_cause"] = "1????????????????????????2??????????????????3????????????????????????4???????????????"
        cell_ab_list["order_status"] = ""
        min_date = min(cell_ab_data["trace_date"])
        cell_ab_list["order_date"] = datetime.date(int(min_date[:4]), int(min_date[4:6]), int(min_date[6:8]))
        cell_ab_list["create_time"] = datetime.datetime.now()
        cell_ab_list["expert_analysis"] = 0
        cell_ab_list["field_test"] = 0
        cell_ab_list["comments"] = ""
        cell_ab_list["province"] = "??????"
        cell_ab_list["city"] = "??????"
        cell_ab_list["order_sorting"] = "??????"
        cell_ab_list["antenna_feeder_type"] = ""
        ##????????????
        cell_ab_list = cell_ab_list[["order_id", "order_code", "fault_catalog", "fault_class", "fault_subclass",
                                     "cell_name", "lon", "lat", "rel_id", "tuning_action", "root_cause", "order_status",
                                     "order_date", "create_time", "rel_id2", "comments", "province", "city",
                                     "date_on_list", "expert_analysis", "field_test", "order_sorting",
                                     "antenna_feeder_type"]]
        postgre_expert(cell_ab_list, "fault_workorder_r2", "cnio")
        print("hout fail cell pairs workorder is inseted into pgsql successful")
    cur.close()
    conn.close()

    print("Postgres Connection release")


def data_date_list():
    ##???????????????????????????????????????????????????????????????7?????????????????????????????????

    now = datetime.date.today()
    date_list = []
    for i in range(7):
        temp = now + datetime.timedelta(days=(-1 - i))
        if temp.month < 10:
            temp_month = "0" + str(temp.month)
        else:
            temp_month = "" + str(temp.month)
        if temp.day < 10:
            temp_day = "0" + str(temp.day)
        else:
            temp_day = str(temp.day)
        temp = str(temp.year) + temp_month + temp_day

        date_list.append(temp)
    return date_list


if __name__ == '__main__':
    threshhold_day = 4
    atenna_reverse_process = mp.Process(name ="atenna_reverse", target=postgre_antenna_reverse,args=[threshhold_day])
    atenna_ab_lobe_process = mp.Process(name ="atenna_ab_lobe", target=postgre_atenna_ab_lobe,args=[threshhold_day])
    atenna_azimuth_error_process = mp.Process(name ="atenna_azimuth_error", target=postgre_atenna_azimuth_error,args=[threshhold_day])
    cell_location_error_process = mp.Process(name ="cell_location_error", target=postgre_cell_location_error,args=[threshhold_day])
    overshooting_process = mp.Process(name ="overshooting", target=postgre_overshooting,args=[threshhold_day])
    high_reest_process = mp.Process(name ="high_reest", target=postgre_high_reest,args=[threshhold_day])
    indoorleak_process = mp.Process(name ="indoorleak", target=postgre_indoorleak,args=[threshhold_day])
    ho_fail_pairs_process = mp.Process(name ="ho_fail", target=postgre_ho_fail_pairs,args=[threshhold_day])


    ## ??????????????????
    atenna_reverse_process.start()
    atenna_ab_lobe_process.start()
    atenna_azimuth_error_process.start()
    cell_location_error_process.start()
    overshooting_process.start()
    high_reest_process.start()
    indoorleak_process.start()
    ho_fail_pairs_process.start()
