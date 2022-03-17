from pyspark.sql import SparkSession
from time import time
import csv
from io import StringIO

spark = SparkSession.builder.appName('broadcast_join').getOrCreate()
sc = spark.sparkContext

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

rdd_1 = sc.textFile("hdfs://master:9000/user/user/files/movie_genres_100.csv") \
	.map(lambda x: split_complex(x))
rdd_2 = sc.textFile("hdfs://master:9000/user/user/files/ratings.csv") \
	.map(lambda x: split_complex(x))

timestamp_1 = time()

rdd_1 = rdd_1 \
	.map(lambda x: (int(x[0]), x[1])) \
	.groupByKey() \
	.mapValues(list)

# map: (movieID, genre)
# groupByKey().mapValues(list): [(movieID, [genre, genre, ...]), ...]

'''
if R < a split of L then
    HR ← build a hash table from R1..Rp
else
    HL1...HLp ← initialize p hash tables for L
===============================================
HR ← build a hash table from R (since partitions are ignored)
'''
rdd_1_hashmap = rdd_1.collectAsMap()
rdd_1_broadcast = sc.broadcast(rdd_1_hashmap) # otherwise it doesn't work

'''
Map(K: null, V: a record from an L split):
    if HR exist then
        probe HR with the join column extracted from V
        for each match r from HR do
            emit(null, new record(r, V))
    else
        add V to an HLi hashing its join column
=======================================================
Map(K: null, V: a record from an L split):
    probe HR with the join column extracted from V
    for each match r from HR do
        emit(null, new record(r, V))
'''
res = rdd_2 \
    .map(lambda x: (int(x[1]), (int(x[0]), float(x[2]), x[3]))) \
    .filter(lambda x: x[0] in rdd_1_broadcast.value) \
    .flatMap(lambda x: ((x[0], (genre, x[1][0], x[1][1], x[1][2])) for genre in rdd_1_broadcast.value[x[0]]))

# map: (movieID, (userID, rating, timestamp))
# flatMap: (movieID, (genre, userID, rating, timestamp))

res.collect()

timestamp_2 = time()
print(timestamp_2 - timestamp_1)