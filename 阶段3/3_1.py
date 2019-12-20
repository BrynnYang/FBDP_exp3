# 统计各省销售最好的产品类别前十（销售最多前10的产品类别）销售对应action：2，对应第7列数据
from pyspark import SparkContext #spark功能入口
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
    task=SparkContext(appName='stage3_issue1')
    #初始RDD
    dataset=task.textFile("million_user_log.csv").map(lambda x:map(x)).cache()
    #过滤得到销售出去的商品
    sales=dataset.filter(lambda x: x[1][2]==2)
    #reduce阶段，key值压缩为省份和商品类别，求和
    res = sales.map(lambda x: ((x[0],x[1][1]),1)).reduceByKey(add)
    #排序
    sort_res = res.map(lambda x: (x[0][0],(x[0][1],x[1]))).groupByKey().mapValues(sort).collect()

    for (key,value) in sort_res:
        print((key,value))
        print('\n')