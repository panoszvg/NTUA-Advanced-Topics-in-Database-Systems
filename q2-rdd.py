from pyspark.sql import SparkSession
from time import time
import csv
from io import StringIO
from pyspark.sql.types import *

spark = SparkSession.builder.appName("q2-rdd").getOrCreate()

sc = spark.sparkContext
rdd = sc.textFile("hdfs://master:9000/user/user/files/ratings.csv")

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

timestamp_1 = time()

# Read rating rows
result = rdd
result = result.map(lambda x: split_complex(x))
result = result.map(lambda row: (row[0], (float(row[2]), 1)))
result = result.reduceByKey(lambda x,y: (x[0] + y[0], x[1] + y[1]))
result = result.map(lambda x: (x[0], x[1][0] / x[1][1]))

# map: returns (key, (rating -> 4.0, instance -> 1))
# reduceByKey: returns (key, (sum of ratings -> 4.0 + 4.5, sum of instances -> 1 + 1))
# map: returns (key, sum of ratings/sum of instances)

res = result.filter(lambda y: y[1] > 3).count() / result.count()
df = spark.createDataFrame([(res, )], StructType([StructField("Users", DoubleType(), False)]))
df.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite').save("hdfs://master:9000/user/user/outputs/q2-rdd.csv")

###########################
timestamp_2 = time()
print(timestamp_2 - timestamp_1)
