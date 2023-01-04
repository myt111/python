import pandas as pd
from sqlalchemy import create_engine
import high_reest_diagnosis as hrd
import ho_fail_diagnosis as hfd
import multiprocessing as mp

def postgre_expert(data,talename,schemaname):
    #engine = create_engine('postgresql://expert-system:YJY_exp#exp502@10.1.77.51:5432/expert-system')
    engine = create_engine('postgresql://expert-system:YJY_exp#exp502@133.160.191.111:5432/expert-system')

    data.to_sql(talename,engine,schema=schemaname,  index=False, if_exists='append',method="multi")


def high_reest_diagnosis():
    # start_day='2022-09-15'
    # end_day = '2022-09-21'

    start_day='2022-09-15'
    end_day = '2022-09-21'
    problem_type = "高重建比例小区"

    ##读取数据
    problem_cell,cell_pm,cell_mr,cell_project_problem,cell_alarm = hrd.db_read(start_day,end_day,problem_type)
    print("告警数据索引".format(cell_alarm.index))
    ##数据关联
    print("数据关联开始")
    hrc_list_add_alarm =hrd.alarm_correlation(problem_cell,cell_alarm)

    hrc_list_add_pm = pd.merge(hrc_list_add_alarm,cell_pm,left_on=["rel_id"],right_on=["eci"],how='left')

    hrc_list_add_mr = pd.merge(hrc_list_add_pm,cell_mr,left_on=["rel_id"],right_on=["eci"],how='left')

    hrc_list_add_project_problem = hrd.project_problem_correlation(hrc_list_add_mr,cell_project_problem)

    print("数据关联结束")

    hrc_diagnose_output= hrd.hrc_diagnose(hrc_list_add_project_problem)

    postgre_expert(hrc_diagnose_output,"workorder_high_reest_cell_diagnosis","cnio")


def ho_fail_diagnosis():

    # start_day='2022-06-13'
    # end_day = '2022-06-19'
    start_day='2022-09-15'
    end_day = '2022-09-21'
    problem_type = "高切换失败小区对"

    ##读取数据
    hof_cell,cell_pm,cell_mr,cell_project_problem,cell_alarm,siteinfo = hfd.db_read(start_day,end_day,problem_type)

    ##数据关联
    print("数据关联开始")
    hofail_cell_add_ho_type = hfd.ho_type(hof_cell,siteinfo)

    hofail_list_add_alarm =hfd.alarm_correlation(hofail_cell_add_ho_type,cell_alarm)

    hofail_list_add_pm = hfd.pm_correlation(hofail_list_add_alarm,cell_pm)

    hofail_list_add_mr = hfd.mr_correlation(hofail_list_add_pm,cell_mr)

    hofail_list_add_project_problem = hfd.project_problem_correlation(hofail_list_add_mr,cell_project_problem)

    print("数据关联结束")
    print("切换失败问题小区诊断 开始")

    hofail_diagnose_output= hfd.ho_diagnosis(hofail_list_add_project_problem)
    
    print("切换失败问题小区诊断 结束")

    postgre_expert(hofail_diagnose_output, "workorder_ho_fail_cell_diagnosis", "cnio")

    print("切换失败问题小区诊断结果入库: 完成")





if __name__ == "__main__":
    #high_reest_diagnosis()
    #ho_fail_diagnosis()
    #数据读取
    high_reest_process = mp.Process(name ="high_reest", target=high_reest_diagnosis)
    ho_fail_pairs_process = mp.Process(name ="ho_fail", target=ho_fail_diagnosis)


    #启动工单进程
    high_reest_process.start()
    ho_fail_pairs_process.start()



    print("分析结束")
