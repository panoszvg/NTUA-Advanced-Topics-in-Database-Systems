from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("q4-sql-csv").getOrCreate()

movies = spark.read.format("csv").options(headers='false', inferSchema='true').load("hdfs://master:9000/user/user/files/movies.csv")
genres = spark.read.format("csv").options(headers='false', inferSchema='true').load("hdfs://master:9000/user/user/files/movie_genres.csv")

movies.registerTempTable("movies")
genres.registerTempTable("genres")

def str_len(str):
    if str is not None and str != '':
        return len(str.split(" "))
    else:
        return None

def time_period(datetime_str):
    if datetime_str is not None:
        year = int(datetime_str.split("-")[0])
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

res = spark.sql("SELECT first(time_period(m._c3)) AS Time_Period, AVG(str_len(m._c2)) AS Average_Summary_Length \
                FROM genres AS g \
                JOIN movies AS m \
                ON g._c0 == m._c0 \
                WHERE (g._c1 == 'Drama' AND YEAR(m._c3) > 1999) \
                GROUP BY time_period(m._c3) ORDER BY Time_Period ASC")

res.show()