import pandas
# DataFrame
df = pandas.read_csv("./gapminder.tsv",sep="\t")
# print(df)
# print(type(df))
# print(df.head(6)) # 显示多少行
#
# print(df.shape) # 显示多少行，多少列

# print(df.shape[0] - 1,df.shape[1])
# print(df.columns)

for i in df.columns:
    print( "columns =>" ,i)

for type in df.dtypes:
    print("type =>" , type)

for item in zip(df.columns,df.dtypes):
    print("item =>" ,item)

result = dict(zip(df.columns,df.dtypes))
country = result.get("country")
print(country)

