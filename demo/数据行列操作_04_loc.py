import pandas
# DataFrame
df = pandas.read_csv("./gapminder.tsv",sep="\t")

# 根据year分组求某列的平均值
# print(df.groupby("year")["gdpPercap"].mean())

# 多组分组统计，求多列的平均值
# print(df.groupby(["year", "continent"])[["lifeExp", "pop"]].mean())
# print(type(df.groupby(["year", "continent"])[["lifeExp", "pop"]].mean())) #<class 'pandas.core.frame.DataFrame'>

year_avg = df.groupby(["year", "continent"])[["lifeExp", "pop"]].mean()

print(year_avg) # 求值
print(year_avg.reset_index()) # 添加索引列

# 1952 Africa     39.135500  4.570010e+06
#      Americas   53.279840  1.380610e+07
#      Asia       46.314394  4.228356e+07
#      Europe     64.408500  1.393736e+07
#      Oceania    69.255000  5.343003e+06
# ===============================================
#     year continent    lifeExp           pop
# 0   1952    Africa  39.135500  4.570010e+06
# 1   1952  Americas  53.279840  1.380610e+07
# 2   1952      Asia  46.314394  4.228356e+07
# 3   1952    Europe  64.408500  1.393736e+07
# 4   1952   Oceania  69.255000  5.343003e+06

##统计数量,统计州国家的数量
print(df.groupby("continent")["country"].nunique())


#统计各个国家取了多少年份的数据--各国取了12年的数据
print(df.groupby("country")["year"].nunique())
