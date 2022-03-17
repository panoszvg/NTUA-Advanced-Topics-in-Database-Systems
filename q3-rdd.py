from pyspark.sql import SparkSession
from time import time
import csv
from io import StringIO
from pyspark.sql.types import *
spark = SparkSession.builder.appName("q3-rdd").getOrCreate()

sc = spark.sparkContext

# rdd_genres = spark.read.csv("hdfs://master:9000/user/user/files/movie_genres.csv").rdd
# rdd_ratings = spark.read.csv("hdfs://master:9000/user/user/files/ratings.csv").rdd

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

rdd_genres = sc.textFile("hdfs://master:9000/user/user/files/movie_genres.csv")
rdd_ratings = sc.textFile("hdfs://master:9000/user/user/files/ratings.csv")

timestamp_1 = time()

rdd_genres = rdd_genres.map(lambda row: split_complex(row))

res = rdd_ratings \
    .map(lambda row: split_complex(row)) \
    .map(lambda row: (row[1], (float(row[2]), 1))) \
    .reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1])) \
	.map(lambda x: (x[0], x[1][0] / x[1][1])) \
    .join(rdd_genres) \
    .map(lambda row: (row[1][1], (float(row[1][0]), 1))) \
    .reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1])) \
	.map(lambda x: (x[0], x[1][0] / x[1][1], x[1][1]))\
	.sortBy(lambda x: x[1],ascending=False)
# After join:
#    862, (3.25, Animation)
#    862, (3.25, Comedy)

# map: returns (key, (rating -> 4.0, instance -> 1))
# reduceByKey: returns (key, (sum of ratings -> 4.0 + 4.5, sum of instances -> 1 + 1))
# map: returns (key, sum of ratings/sum of instances)




df = spark.createDataFrame(res, ["Category", "Avg Rating", "Total Movies"])
df.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite').save("hdfs://master:9000/user/user/outputs/q3-rdd.csv")


timestamp_2 = time()
print(timestamp_2 - timestamp_1)
for i in res.take(10):
	print(i)
