from pyspark.sql import SparkSession
from time import time
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--csv', action='store_true', default=True)
parser.add_argument('-p', '--parquet', action='store_true') # if this option is given, negates csv
args = parser.parse_args()
if args.parquet:
    args.csv = False
    spark = SparkSession.builder.appName("q3-sql").config("spark.sql.parquet.binaryAsString","true").getOrCreate()
    ratings = spark.read.parquet("hdfs://master:9000/user/user/files/ratings.parquet")
    genres = spark.read.parquet("hdfs://master:9000/user/user/files/movie_genres.parquet")

if args.csv:
    spark = SparkSession.builder.appName("q3-sql").getOrCreate()
    ratings = spark.read.format("csv").options(headers='false', inferSchema='true').load("hdfs://master:9000/user/user/files/ratings.csv")
    genres = spark.read.format("csv").options(headers='false', inferSchema='true').load("hdfs://master:9000/user/user/files/movie_genres.csv")

ratings.registerTempTable("ratings")
genres.registerTempTable("genres")

timestamp_1 = time()

res = spark.sql("SELECT g._c1 AS Genre, AVG(r.average_rating) AS Average_Rating, COUNT(*) AS No_of_Movies_in_Genre FROM genres AS g JOIN (SELECT _c1, AVG(r._c2) AS average_rating FROM ratings AS r GROUP BY r._c1) r ON g._c0 == r._c1 GROUP BY g._c1 ORDER BY Average_Rating DESC")
res.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite').save("hdfs://master:9000/user/user/outputs/q3-sql.csv")

timestamp_2 = time()
print(timestamp_2 - timestamp_1)
