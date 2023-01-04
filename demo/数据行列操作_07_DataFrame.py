from pandas import DataFrame

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

# print(df.index)
# dfloc = df.loc["1"]
#
# print(dfloc)
# print(type(dfloc)) # <class 'pandas.core.series.Series'>
# print("-------")
# print(df.keys()) # Index(['name', 'position', 'birthday', 'died', 'age'], dtype='object')pr
# print("-------")
# print(dfloc.keys())
# print("-------")
# print(dfloc.values)

print(df.loc["1"].values)  # 根据索引取某一行的数据

# 有顺序的字段
# from collections import OrderedDict
#
# df = DataFrame(OrderedDict([
#
# 	("name", ["胡晓水", "闫凯瑞"]),
# 	("position", ["班长", "学习委员"]),
# 	("birthday", ["1995-04-06", "1996-08-15"]),
# 	("died", ["2095-04-06", "2095-08-06"]),
# 	("age", [100, 103])
#
# ]),index=["胡","闫"])
#
# print(df)
