import pandas
# DataFrame
df = pandas.read_csv("./gapminder.tsv",sep="\t")
#
# # 获取行loc 不支持负数，iloc 支持负数
# print(df.loc[0])
# print(df.iloc[0])
# # 目标： 从第三行开始取数据
# print(df.loc[2])
#
# # 取倒数第三条
# print(df.iloc[-3])
#
# # 取最后一行
# last_index = df.shape[0] - 1
# print(df.loc[last_index])
#
# print(df.iloc[-1])

# 取第二行以后的数
print(df.loc[1:,])