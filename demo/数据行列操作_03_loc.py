import pandas
# DataFrame
df = pandas.read_csv("./gapminder.tsv",sep="\t")

# 取第二行以后的数，列不受限制
print(df.loc[1:,])

#行不受限制，取某列
print(df.loc[:, ["year", "country"]])
print(type(df.loc[:, ["year", "country"]])) # <class 'pandas.core.frame.DataFrame'>

# df.loc[行,列]

# 使用列的下表，取低第几列，从0开始，下标只有iloc支持
print(df.iloc[:, [2, 3]])

# 取行下标2，6 列下标3，6
print(df.iloc[2:6, 3:5]) # 左闭右开，不包括表头

# 取第0行，某列数据
print(df.loc[0, ["year"]]) # year    1952 一维
print(type(df.loc[0, ["year"]])) # <class 'pandas.core.series.Series'>
print(df.loc[0:3, ["year", "pop"]]) # 行包含 0 1 2 3
print(df.iloc[0:3, [2, 5]]) # 行不包含 0 1 2