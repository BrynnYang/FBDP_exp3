#第三个问题在hive作业里也出现过，因此尝试使用sparkSQL，结果发现非常方便
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName('stage_issue3').getOrCreate()
    #sql.csv是加了小标题的csv，header是指是否指定头部行作为schema
    dataset = spark.read.csv("sql.csv", header=True)
    dataset.createOrReplaceTempView("dataset")
    data = spark.sql("select brand_id,count(brand_id) number from dataset where action=0 group by brand_id order by number desc limit 10").show()

