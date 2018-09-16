import pyspark
from pyspark import SparkContext

sc = SparkContext(appName='PythonADMM')

rdd = sc.textFile("C:/Users/Admin/PycharmProjects/ADMM/file/*").mapPartitionsWithIndex(
    lambda key, iterator: [(key, (key, key))])


# stringRDD = rdd.flatMap(lambda line: line.split(" "))
# words = stringRDD.map(lambda word: (word, 1))
# print(words.reduceByKey(lambda x, y: x + y).collect())


def add1(key, iterator):
    newlist = list()
    for i in iterator:
        newlist.append(i + 1)

    return [(key, newlist)]


z = list()


def merge(x, y):
    print(x)
    print(y)
    return x+y


data = [1, 2, 3, 4, 5, 6, 7, 8, 9]
rddData = sc.parallelize(data, 3).mapPartitionsWithIndex(add1).values().reduce(merge)

# print(rddData.getNumPartitions())
# print(rddData.collect())

print(rddData)
