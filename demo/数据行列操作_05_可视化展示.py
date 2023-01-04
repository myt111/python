import pandas
import matplotlib.pyplot as plt
from matplotlib import font_manager

# DataFrame
df = pandas.read_csv("./gapminder.tsv",sep="\t")
gloable_year_lifeExp = df.groupby("year")["lifeExp"].mean()
print(gloable_year_lifeExp)

gloable_year_lifeExp.plot()

zhfont = font_manager.FontProperties(fname='C:\Windows\Fonts\simsun.ttc')

#显示示例
plt.legend(["avg"])


#添加标题
plt.title("全球按年分组后的平均寿命",fontproperties=zhfont)

#显示
plt.show()