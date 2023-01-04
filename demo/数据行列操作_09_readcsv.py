from pandas import DataFrame
from pandas import read_csv
df = read_csv("scientists.csv",sep=",")
print(type(df))
print(df)

age = df["Age"] # 取一列
print(age)

dfcolumes = df[["Age","Name"]]
print(dfcolumes)
print(dfcolumes.sort_values(ascending=False, by="Age")) # 根据年龄从大到小排序

