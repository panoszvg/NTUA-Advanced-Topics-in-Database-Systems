from pyspark.sql import SparkSession
from pyspark.sql.types import *
from time import time
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--csv', action='store_true', default=True)
parser.add_argument('-p', '--parquet', action='store_true') # if this option is given, negates csv
args = parser.parse_args()
if args.parquet:
    args.csv = False
    spark = SparkSession.builder.appName("q4-sql").config("spark.sql.parquet.binaryAsString","true").getOrCreate()
    movies = spark.read.parquet("hdfs://master:9000/user/user/files/movies.parquet")
    genres = spark.read.parquet("hdfs://master:9000/user/user/files/movie_genres.parquet")

if args.csv:
    spark = SparkSession.builder.appName("q4-sql").getOrCreate()
    movies = spark.read.format("csv").options(headers='false', inferSchema='true').load("hdfs://master:9000/user/user/files/movies.csv")
    genres = spark.read.format("csv").options(headers='false', inferSchema='true').load("hdfs://master:9000/user/user/files/movie_genres.csv")

movies.registerTempTable("movies")
genres.registerTempTable("genres")


def str_len(str):
    if str is not None:
        return len(str.split(" "))
    else:
        return None

def time_period(datetime_str):
    if datetime_str is not None:
        year = int(datetime_str)
        if year < 2005:
            return "2000-2004"
        elif year < 2010:
            return "2005-2009"
        elif year < 2015:
            return "2010-2014"
        elif year < 2020:
            return "2015-2019"
    else:
        return None


spark.udf.register("str_len", str_len, IntegerType())
spark.udf.register("time_period", time_period)

timestamp_1 = time()

res = spark.sql("SELECT first(time_period(YEAR(m._c3))) AS Time_Period, AVG(str_len(m._c2)) AS Average_Summary_Length \
                FROM genres AS g \
                JOIN movies AS m \
                ON g._c0 == m._c0 \
                WHERE (g._c1 == 'Drama' AND YEAR(m._c3) > 1999) \
                GROUP BY time_period(YEAR(m._c3)) ORDER BY Time_Period ASC")

res.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite').save("hdfs://master:9000/user/user/outputs/q4-sql.csv")

timestamp_2 = time()
print(timestamp_2 - timestamp_1)