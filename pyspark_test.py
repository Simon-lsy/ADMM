import pyspark
from pyspark import SparkContext
import numpy as np


def parse_vector(line):
    return np.fromstring(line, sep=',')


def getParameter(key, iterator):
    newlist = list()
    for i in iterator:
        newlist += list(i)

    return [(key, newlist)]


def combine(x, y):
    return x + y


sc = SparkContext(appName='PythonADMM')

rdd = sc.textFile("C:/Users/Admin/PycharmProjects/ADMM/file/*").map(parse_vector)
print(rdd.getNumPartitions())

parameterPartition = rdd.mapPartitionsWithIndex(getParameter)
print(parameterPartition.collect())


def updateParameter(value):
    parameterNum = len(value)
    for i in range(parameterNum):
        value[i] = value[i] * 2
    return value


updatedParameter = parameterPartition.mapValues(updateParameter)
print(updatedParameter.collect())

parameterZ = updatedParameter.values().reduce(combine)

# print(rdd.collect())
print(parameterZ)


# stringRDD = rdd.flatMap(lambda line: line.split(" "))
# words = stringRDD.map(lambda word: (word, 1))
# print(words.reduceByKey(lambda x, y: x + y).collect())


def add1(key, iterator):
    newlist = list()
    for i in iterator:
        newlist.append(i + 1)

    return [(key, newlist)]


data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
rddData = sc.parallelize(data, 3).mapPartitionsWithIndex(add1).values().reduce(combine)
print(rddData)
