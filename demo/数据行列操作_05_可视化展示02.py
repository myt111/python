from pandas import read_csv
import matplotlib.pyplot as plt

df = read_csv("gapminder.tsv",sep="\t")

#全球按年分组后的平均寿命
gloable_year_lifeExp = df.groupby("year")["lifeExp"].mean()

#全球按年分组后的平均GDP
gloable_year_gdpPercap = df.groupby("year")["gdpPercap"].mean()


#把两个图合并放在一起
#第一个参数，放在一行，第二参数是每行放2个图，第三方个参数，图片宽和高
fig,(ax1,ax2) = plt.subplots(1,2,figsize=(8,4))

#设置数据
ax1.plot(gloable_year_lifeExp)
ax2.plot(gloable_year_gdpPercap)

#设置标题
ax1.set_title("全球按年分组后的平均寿命")
ax2.set_title("全球按年分组后的平均GDP")


#设置示例
ax1.legend(["avglife"])
ax2.legend(["avggdp"])



#显示
plt.show()

