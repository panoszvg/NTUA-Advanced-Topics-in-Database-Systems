from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("q1-rdd").getOrCreate()

sc = spark.sparkContext

rdd = spark.read.csv("hdfs://master:9000/user/user/files/movies.csv").rdd

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


for i in res.collect():
	print(i)