import pyspark
from pyspark import SparkContext
import numpy as np

sc = SparkContext(appName='PythonADMM')


def getParameterRange(key, iterator):
    pararange = {}
    for line in iterator:
        para = line.split(',')
        pararange[para[0]] = para[1:]
        # print(pararange)
    # return [key, (key, pararange)]
    return [(key, pararange)]


def getChangeableCellIdList(key, iterator):
    changeableCellIdList = list()
    for cellId in iterator:
        changeableCellIdList.append(cellId)
    return [(key, changeableCellIdList)]


rdd = sc.textFile("C:/Users/Admin/PycharmProjects/ADMM/BCD/Cell_Parameter_Range/*").mapPartitionsWithIndex(
    getParameterRange)
print(rdd.collect())

print(rdd.getNumPartitions())

rdd1 = sc.textFile("C:/Users/Admin/PycharmProjects/ADMM/BCD/ChangeableCellIDList/*").mapPartitionsWithIndex(
    getChangeableCellIdList)

print(rdd1.collect())

rdd2 = rdd.join(rdd1, 2)
print(rdd2.collect())
print(rdd2.getNumPartitions())


def getXinit(iterator):
    dim = len(list(iterator[1])) * 4
    print(dim)
    xlow = np.zeros(dim, )
    xup = np.zeros(dim, )
    xinit = np.zeros(dim, )
    print(len(iterator))
    print(iterator[0])
    print(iterator[1])
    for idx, cellID in enumerate(list(iterator[1])):
        # print(cellID)
        line = iterator[0][cellID]
        xlow[4 * idx:4 * idx + 4] = np.array([float(line[0]), float(line[2]), float(line[4]), float(line[6])])
        xup[4 * idx:4 * idx + 4] = np.array([float(line[1]), float(line[3]), float(line[5]), float(line[7])])
        xinit[4 * idx:4 * idx + 4] = np.array([float(line[8]), float(line[9]), float(line[10]), float(line[11])])
    return (dim, xlow, xup, xinit)


rdd3 = rdd2.mapValues(getXinit)

print(rdd3.collect())
