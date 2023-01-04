from pandas import DataFrame
from pandas import read_csv
# 创建DataFrame对象
# DataFram里是字典，加列固定列的顺序，加索引有两行数据，第几行下标
df = DataFrame({
    "name": ["胡晓水", "闫凯瑞"],
    "position": ["班长", "学习委员"],
    "birthday": ["1995-04-06", "1996-08-15"],
    "died": ["2095-04-06", "2095-08-06"],
    "age": [100, 103]

}, columns=["name", "position", "birthday", "died", "age"],
    index=["1", "2"]

)

print(df)

age = df["age"]
print(age.mean())

print(age.max())
print(age.min())
# 方差
print(age.std())
# 从大到小排序
print(age.sort_values(ascending=False))

print(age.append(age))