from pyspark.sql import SparkSession
from time import time
import csv
from io import StringIO
from pyspark.sql.types import *


spark = SparkSession.builder.appName("q1-rdd").getOrCreate()
sc = spark.sparkContext

rdd = spark.read.csv("hdfs://master:9000/user/user/files/movies.csv").rdd

timestamp_1 = time()

def filter(row):
	if int(row._c5) != 0 and int(row._c6) != 0 and row._c3 is not None and int(row._c3.split("-")[0]) >= 2000:
		return True
	else:
		return False

res = rdd \
	.filter(lambda row : filter(row)) \
	.map(lambda row : (int(row._c3.split("-")[0]), (((int(row._c6)-int(row._c5))/int(row._c5))*100, row._c0, row._c1))) \
	.reduceByKey(lambda x,y : max(x,y)) \
	.sortByKey() \
	.map(lambda row: (row[0], row[1][1], row[1][2], row[1][0]))

res.toDF().show()

df = spark.createDataFrame(res, ["Year","Id","Title","Profit"])
df.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite').save("hdfs://master:9000/user/user/outputs/q1-rdd.csv")

timestamp_2 = time()
print(timestamp_2 - timestamp_1)
