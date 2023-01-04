from pandas import read_csv

from pandas import Series

s1 = Series([22,333,555])
print(s1) # 里面是int64
print(type(s1)) # 列表类型


s = Series(["张三","老师"])
print(s)
print(s.index)
print(type(s))
ss = Series(["张三","老师"],index=["姓名","职位"])
print(ss)
print(ss.index)
print(type(ss))

