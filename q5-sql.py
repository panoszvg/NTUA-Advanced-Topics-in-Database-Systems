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
    spark = SparkSession.builder.appName("q5-sql").config("spark.sql.parquet.binaryAsString","true").getOrCreate()
    movies = spark.read.parquet("hdfs://master:9000/user/user/files/movies.parquet")
    genres = spark.read.parquet("hdfs://master:9000/user/user/files/movie_genres.parquet")
    ratings = spark.read.parquet("hdfs://master:9000/user/user/files/ratings.parquet")

if args.csv:
    spark = SparkSession.builder.appName("q5-sql").getOrCreate()
    movies = spark.read.format("csv").options(headers='false', inferSchema='true').load("hdfs://master:9000/user/user/files/movies.csv")
    genres = spark.read.format("csv").options(headers='false', inferSchema='true').load("hdfs://master:9000/user/user/files/movie_genres.csv")
    ratings = spark.read.format("csv").options(headers='false', inferSchema='true').load("hdfs://master:9000/user/user/files/ratings.csv")

movies.registerTempTable("movies")
genres.registerTempTable("genres")
ratings.registerTempTable("ratings")

timestamp_1 = time()

# add genres to movies and get necessary rows
moviesWithGenres = spark.sql("SELECT m._c0 AS Id, m._c1 AS Title, m._c7 AS Popularity, g._c1 AS Genre FROM movies AS m FULL OUTER JOIN genres AS g ON (m._c0 == g._c0)")
moviesWithGenres.registerTempTable("moviesWithGenres")

# Count all ratings grouped by genre and user 
countRatings = spark.sql("SELECT g._c1 AS Genre, r._c0 AS User, COUNT(*) AS Rating FROM ratings AS r FULL OUTER JOIN genres AS g ON r._c1 == g._c0 WHERE (g._c1 IS NOT NULL AND r._c0 IS NOT NULL) GROUP BY g._c1, r._c0")
countRatings.registerTempTable("countRatings")
# Get max # of ratings of a user for each genre
maxRatings = spark.sql("SELECT Genre, MAX(Rating) AS Rating FROM countRatings GROUP BY countRatings.Genre")
maxRatings.registerTempTable("maxRatings")
# find user with max # of ratings for each genre (and the # of ratings)
maxRatingsPerUser = spark.sql("SELECT User, Genre, Rating FROM (SELECT Genre, User, Rating, ROW_NUMBER() OVER (PARTITION BY Genre ORDER BY User DESC) AS rn FROM countRatings JOIN maxRatings USING (Genre, Rating) ORDER BY User DESC) preMaxRatingsPerUser WHERE rn == 1") # row number needed for duplicate valuesin # of ratings - get user with higher id
maxRatingsPerUser.registerTempTable("maxRatingsPerUser")
# add genres to movies and get necessary rows
moviesWithGenres = spark.sql("SELECT m._c0 AS Id, m._c1 AS Title, m._c7 AS Popularity, g._c1 AS Genre FROM movies AS m FULL OUTER JOIN genres AS g ON (m._c0 == g._c0)")
moviesWithGenres.registerTempTable("moviesWithGenres")
# make tables needed to run queries
userBestMovies = spark.sql("SELECT r._c0 AS User, r._c2 AS Rating, Title, Popularity, Genre FROM ratings AS r JOIN moviesWithGenres AS mg ON (r._c1 == mg.Id) ORDER BY r._c2 DESC, mg.Popularity DESC")
userBestMovies.registerTempTable("userBestMovies")
userWorstMovies = spark.sql("SELECT r._c0 AS User, r._c2 AS Rating, Title, Popularity, Genre FROM ratings AS r JOIN moviesWithGenres AS mg ON (r._c1 == mg.Id) ORDER BY r._c2 ASC, mg.Popularity DESC")
userWorstMovies.registerTempTable("userWorstMovies")
bestAndWorstPerUser = spark.sql("SELECT b.Genre, b.User, b.Title AS BestMovie, b.Rating AS BestRating, w.Title AS WorstMovie, w.Rating AS WorstRating FROM userBestMovies AS b JOIN userWorstMovies AS w ON (b.User == w.User AND b.Genre == w.Genre)")
bestAndWorstPerUser.registerTempTable("bestAndWorstPerUser")

# run query
res = spark.sql("SELECT m.Genre AS Genre, first(m.User) AS User, first(m.Rating) AS No_of_ratings, first(u.BestMovie) AS Favourite_Movie, first(u.BestRating) AS Favourite_Movie_Rating, first(u.WorstMovie) AS Least_Favourite_Movie, first(u.WorstRating) AS Least_Favourite_Movie_Rating FROM maxRatingsPerUser AS m JOIN bestAndWorstPerUser AS u ON (m.User == u.User AND m.Genre == u.Genre) GROUP BY m.Genre ORDER BY Genre ASC")
res.coalesce(1).write.format("com.databricks.spark.csv").mode('overwrite').save("hdfs://master:9000/user/user/outputs/q5-sql.csv")

timestamp_2 = time()
print(timestamp_2 - timestamp_1)