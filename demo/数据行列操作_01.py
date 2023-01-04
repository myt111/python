import pandas
# DataFrame
df = pandas.read_csv("./gapminder.tsv",sep="\t")

# 获取一列
country = df["country"]
# print(country)
# # <class 'pandas.core.series.Series'> 一维数据类型
# print(type(country))
#
# print(country.shape)

print(df.head()) # 默认首5行
print(df.tail()) # 默认尾5行
print(df.shape[0]) # 多少行
print(df.shape[1]) # 多少列

columns = df[["country","year"]]
print(columns)
