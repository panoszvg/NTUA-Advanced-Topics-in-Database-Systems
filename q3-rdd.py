from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("q3-rdd").getOrCreate()

sc = spark.sparkContext

rdd_genres = spark.read.csv("hdfs://master:9000/user/user/files/movie_genres.csv").rdd
rdd_ratings = spark.read.csv("hdfs://master:9000/user/user/files/ratings.csv").rdd

res = rdd_ratings \
    .map(lambda row: (row._c1, (float(row._c2), 1))) \
    .reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1])) \
	.map(lambda x: (x[0], x[1][0] / x[1][1])) \
    .join(rdd_genres) \
    .map(lambda row: (row[1][1], (float(row[1][0]), 1))) \
    .reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1])) \
	.map(lambda x: (x[0], x[1][0] / x[1][1], x[1][1]))

# After join:
#    862, (3.25, Animation)
#    862, (3.25, Comedy)

# map: returns (key, (rating -> 4.0, instance -> 1))
# reduceByKey: returns (key, (sum of ratings -> 4.0 + 4.5, sum of instances -> 1 + 1))
# map: returns (key, sum of ratings/sum of instances)

for i in res.take(5):
	print(i)