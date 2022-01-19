from pyspark.sql import SparkSession
from operator import add

spark = SparkSession.builder.appName("q2-rdd").getOrCreate()

sc = spark.sparkContext

rdd = spark.read.csv("hdfs://master:9000/user/user/files/ratings.csv").rdd

def format(row):
	if row._c0 is not None and row._c2 is not None and row._c0 != '' and row._c2 != '':
		return (int(row._c0), (float(row._c2), 1))
	else:
		return None

res = rdd \
	.map(lambda row : format(row)) \
	.reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1])) \
	.map(lambda x: (x[0], x[1][0] / x[1][1])) \
	.filter(lambda y: y[1] > 3) \
	.count() / rdd.keys().distinct().count()

# map: returns (key, (rating -> 4.0, instance -> 1))
# reduceByKey: returns (key, (sum of ratings -> 4.0 + 4.5, sum of instances -> 1 + 1))
# map: returns (key, sum of ratings/sum of instances)

# for i in res.take(5):
# 	print(i)
print(res)