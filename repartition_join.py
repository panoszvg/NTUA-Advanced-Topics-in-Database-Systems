from pyspark.sql import SparkSession
from time import time
import csv
from io import StringIO

spark = SparkSession.builder.appName('repartition_join').getOrCreate()
sc = spark.sparkContext

def split_complex(x):
    return list(csv.reader(StringIO(x), delimiter=','))[0]

rdd_1 = sc.textFile("hdfs://master:9000/user/user/files/movie_genres_100.csv") \
    .map(lambda x: split_complex(x))
rdd_2 = sc.textFile("hdfs://master:9000/user/user/files/ratings.csv") \
    .map(lambda x: split_complex(x))

'''
Reduce (K: a join key,LIST V: records from R and L with join key K):
    create buffers BR and BL for R and L, respectively
    for each record t in LIST V do
        append t to one of the buffers according to its tag
    for each pair of records (r, l) in BR x BL do
        emit (null, new record(r, l))               # add them to list and return list
'''
def joinRDDs(tuple):

    buffer_1 = []
    buffer_2 = []
    result = []

    for pair in tuple[1]: # listV
        if pair[1] == 1:
            buffer_1.append(pair[0])
        else:
            buffer_2.append(pair[0])

    for r in buffer_1:
        for l in buffer_2:
            result.append((r, l))

    return result


'''
Map (K: null, V : a record from a split of either R or L)
    join key ← extract the join column from V
    tagged record ← add a tag of either R or L to V
    emit (join key, tagged record)
'''

timestamp_1 = time()

rdd_1 = rdd_1.map(lambda row: (row[0], (row, 1))) # column 0 on movie_genres_100
rdd_2 = rdd_2.map(lambda row: (row[1], (row, 2))) # column 1 on ratings

res = rdd_1 \
    .union(rdd_2) \
    .groupByKey() \
    .mapValues(list) \
    .map(joinRDDs) \
    .filter(lambda x: len(x) > 0) \
    .flatMap(lambda x: x) 

# union: [rdd_1, rdd_2]
# groupByKey.mapValues(list): [(movieID, [([row], tag), ([row], tag), ...]), ...]
# map(joinRDDs): essentially performs cross product on all items of list above (rdd_1 x rdd_2)

res.collect()

timestamp_2 = time()
print(timestamp_2 - timestamp_1)