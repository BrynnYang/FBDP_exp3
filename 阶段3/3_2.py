# 统计各省的双十一前十热门销售产品（购买最多前10的产品）
# 和前一问的思路完全一致，但是key值压缩的时候把商品类别换成商品id即可
from pyspark import SparkContext
from operator import add

def map(x):
    x = x.strip()
    s = x.split(',')
    return (s[10],[s[1],s[2],int(s[7])]) #省份，商品id，商品类别，消费者行为

def sort(y):
    commodities=sorted(y,key=lambda x: x[1],reverse=True)
    #对每个商品类别的数量排序
    top10_sales=commodities[:10]
    return top10_sales

if __name__ == "__main__":

    # 任务名称
    task=SparkContext(appName='stage3_issue2')
    #初始RDD
    dataset=task.textFile("million_user_log.csv").map(lambda x:map(x)).cache()
    #过滤得到销售出去的商品
    sales=dataset.filter(lambda x: x[1][2]==2)
    #reduce阶段，key值压缩为省份和商品id，求和
    res = sales.map(lambda x: ((x[0],x[1][0]),1)).reduceByKey(add)
    #排序
    sort_res = res.map(lambda x: (x[0][0],(x[0][1],x[1]))).groupByKey().mapValues(sort).collect()

    for (key,value) in sort_res:
        print((key,value))
        print('\n')