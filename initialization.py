import pyspark
from pyspark import SparkContext
import numpy as np
import math

sc = SparkContext(appName='PythonADMM')

PARTITON_NUM = 4
PI = 3.1416
RSRPThre = -90
SINRThre = 3
RSRPLambda = 2
SINRLambda = 2
paraType = ['ElectricalTilt', 'RS Power', 'MechanicalTilt', 'Azimuth']


def getParameterRange(key, iterator):
    pararange = {}
    for line in iterator:
        para = line.split(',')
        pararange[para[0]] = para[1:]
    # return [key, (key, pararange)]
    return [(key, pararange)]


def getChangeableCellIdList(key, iterator):
    changeableCellIdList = list()
    for cellId in iterator:
        changeableCellIdList.append(cellId)
    return [(key, changeableCellIdList)]


Cell_Parameter_Range_RDD = sc.textFile("BCD/Cell_Parameter_Range/*").mapPartitionsWithIndex(getParameterRange)
print(Cell_Parameter_Range_RDD.collect())
print(Cell_Parameter_Range_RDD.getNumPartitions())

ChangeableCellIDListRDD = sc.textFile("BCD/ChangeableCellIDList/*").mapPartitionsWithIndex(getChangeableCellIdList)


def getXinit(iterator):
    dim = len(list(iterator[1])) * 4
    print(dim)
    xlow = np.zeros(dim, )
    xup = np.zeros(dim, )
    xinit = np.zeros(dim, )
    for idx, cellID in enumerate(list(iterator[1])):
        line = iterator[0][cellID]
        xlow[4 * idx:4 * idx + 4] = np.array([float(line[0]), float(line[2]), float(line[4]), float(line[6])])
        xup[4 * idx:4 * idx + 4] = np.array([float(line[1]), float(line[3]), float(line[5]), float(line[7])])
        xinit[4 * idx:4 * idx + 4] = np.array([float(line[8]), float(line[9]), float(line[10]), float(line[11])])
    return {"dim": dim, 'xlow': xlow, 'xup': xup, 'xinit': xinit}


XRangeRDD = Cell_Parameter_Range_RDD.join(ChangeableCellIDListRDD, 2).mapValues(getXinit)
print(XRangeRDD.collect())
print(XRangeRDD.getNumPartitions())


def GetGridList(index, iterator):
    gridList = dict()
    for line in iterator:
        grid = dict()
        gridID = ''
        a = line[:-1]
        A = a.split(',')
        for i in A:
            b = i.split(':')
            key = b[0]
            try:
                value = b[1]
            except:
                continue
            if key == 'ID':
                gridID = value
            else:
                grid[key] = value
        gridList[gridID] = grid
    return [(index, gridList)]


def GetCellList(index, iterator):
    cellList = dict()
    for line in iterator:
        cellID = ''
        cell = dict()
        a = line[:-2]
        A = line.split(',')
        for i in A:
            b = i.split(':')
            key = b[0]
            try:
                value = b[1]
            except:
                continue
            if key == 'ID':
                cellID = value
            else:
                cell[key] = value
        cellList[cellID] = cell
    return [(index, cellList)]


def GetCellIdList(index, iterator):
    cellIdList = list()
    for line in iterator:
        cellIdList.append(line)
    return [(index, cellIdList)]


def GetGridCellRSRP(index, iterator):
    gridCellRSRPList = dict()
    data = dict()
    data['GridID'] = list()
    data['CellID'] = list()
    data['TotalRSRP'] = list()
    for line in iterator:
        line = line.strip('\n').split('\t')
        data['GridID'].append(line[0])
        data['CellID'].append(line[1])
        data['TotalRSRP'].append(float(line[2]))
    for i in range(len(data['GridID'])):
        if data['GridID'][i] in gridCellRSRPList:
            gridCellRSRPList[data['GridID'][i]][data['CellID'][i]] = data['TotalRSRP'][i]
        else:
            cellRSRP = dict()
            cellRSRP[data['CellID'][i]] = data['TotalRSRP'][i]
            gridCellRSRPList[data['GridID'][i]] = cellRSRP
    return [(index, gridCellRSRPList)]


def GetCellAntennaGainList(cellList):
    cellAntennaGainList = dict()
    for key, value in cellList.items():
        filename = 'BCD/' + value['AntennaModel'] + '.txt'
        type = 0
        cellAntennaGain = dict()
        antennaH = list()
        antennaV = list()
        for a in open(filename, 'r'):
            a.strip()
            if 'HORIZONTAL' in a or 'VERTICAL' in a:
                type += 1
            elif type == 0 and 'GAIN' in a:
                # print a
                x, y, z = a.split(' ')
                cellAntennaGain['Gain'] = float(y)
            elif type == 1:
                x, y = a.split(' ')
                antennaH.append(float(y))
            elif type == 2:
                x, y = a.split(' ')
                antennaV.append(float(y))
        cellAntennaGain['H'] = antennaH
        cellAntennaGain['V'] = antennaV
        cellAntennaGainList[key] = cellAntennaGain
    return cellAntennaGainList


def GetEarthAzimuth(gridLon, gridLat, cellLon, cellLat):
    dx = float(gridLon) - float(cellLon)
    dy = float(gridLat) - float(cellLat)
    earthAzimuth = 0
    if dy == 0:
        if dx >= 0:
            earthAzimuth = PI / 2
        else:
            earthAzimuth = PI * 3 / 2
    elif dx >= 0 and dy > 0:
        earthAzimuth = math.atan(abs(dx / dy))
    elif dx >= 0 and dy < 0:
        earthAzimuth = PI - math.atan(abs(dx / dy))
    elif dx < 0 and dy < 0:
        earthAzimuth = PI + math.atan(abs(dx / dy))
    else:
        earthAzimuth = 2 * PI - math.atan(abs(dx / dy))
    return earthAzimuth


def GetEarthTile(gridLon, gridLat, cellLon, cellLat, cellHeight):
    dx = float(gridLon) - float(cellLon)
    dy = float(gridLat) - float(cellLat)
    ddx = dx * 111000 * math.cos(float(gridLat))
    ddy = dy * 111000
    dis = math.sqrt(ddx * ddx + ddy * ddy)
    earthTile = math.atan(float(cellHeight) / dis)
    return earthTile


def CalcH(cellAntennaGainList, cellID, x):
    mmin = int(x)
    mmax = mmin + 1
    minx = (mmin + 360) % 360
    maxx = (mmax + 360) % 360
    return cellAntennaGainList[cellID]['H'][minx] * (mmax - x) + cellAntennaGainList[cellID]['H'][maxx] * (x - mmin)


def CalcV(cellList, cellAntennaGainList, cellID, x):
    if x <= 90:
        x -= float(cellList[cellID]['ElectricalTilt'])
    else:
        x += float(cellList[cellID]['ElectricalTilt'])
    mmin = int(x)
    mmax = mmin + 1
    minx = (mmin + 360) % 360
    maxx = (mmax + 360) % 360
    return cellAntennaGainList[cellID]['V'][minx] * (mmax - x) + cellAntennaGainList[cellID]['V'][maxx] * (x - mmin)


def GetGridCellAntennaGain(cellList, gridList, cellAntennaGainList, cellID, gridID):
    aTX = cellList[cellID]['Azimuth']
    eTX = cellList[cellID]['MechanicalTilt']
    aRX = GetEarthAzimuth(gridList[gridID]['Longitude'], gridList[gridID]['Latitude'],
                          cellList[cellID]['Longitude'], cellList[cellID]['Latitude'])
    eRX = GetEarthTile(gridList[gridID]['Longitude'], gridList[gridID]['Latitude'],
                       cellList[cellID]['Longitude'], cellList[cellID]['Latitude'],
                       cellList[cellID]['AntennaHeight'])
    aTX = float(aTX)
    eTX = float(eTX)
    aRX = float(aRX)
    eRX = float(eRX)
    if aRX == aTX:
        az = 0
        el = eRX - eTX
    else:
        az = math.atan(1 / (math.cos(eTX) / math.tan(aRX - aTX) + math.sin(eTX) * math.tan(eRX) / math.sin(aRX - aTX)))
        el = math.atan(
            math.sin(az) * (math.cos(eTX) * math.tan(eRX) / math.sin(aRX - aTX) - math.sin(eTX) / math.tan(aRX - aTX)))
    if math.sin(az) * math.sin(aRX - aTX) < 0:
        az += PI
    az = az * 180 / PI
    el = el * 180 / PI
    gain = CalcH(cellAntennaGainList, cellID, az) - ((180 - math.fabs(az)) / 180 * (
            CalcH(cellAntennaGainList, cellID, 0) - CalcV(cellList, cellAntennaGainList, cellID, el)) + math.fabs(
        az) / 180 * (CalcH(cellAntennaGainList, cellID, 180) - CalcV(cellList, cellAntennaGainList, cellID, 180 - el)))
    gain += cellAntennaGainList[cellID]['Gain'] - max(cellAntennaGainList[cellID]['H'])
    return gain


def GetGridCellLoss(value):
    cellList = value['cellList']
    gridList = value['gridList']
    gridCellRSRPList = value['gridCellRSRPList']
    cellAntennaGainList = value['cellAntennaGainList']
    gridCellLossList = dict()
    for gridID, cellRSRPList in gridCellRSRPList.items():
        for cellID, cellRSRP in cellRSRPList.items():
            cellID = str(cellID)
            cellGain = GetGridCellAntennaGain(cellList, gridList, cellAntennaGainList, cellID, gridID)
            loss = float(cellList[cellID]['RS Power']) + cellGain - float(cellRSRP)
            if gridID in gridCellLossList:
                gridCellLossList[gridID][cellID] = loss
            else:
                cellLoss = dict()
                cellLoss[cellID] = loss
                gridCellLossList[gridID] = cellLoss
    return gridCellLossList


cellListRDD = sc.textFile("BCD/CellList/*").mapPartitionsWithIndex(GetCellList)
print(cellListRDD.collect())
print(cellListRDD.getNumPartitions())

cellIdListRDD = sc.textFile("BCD/CellIDList/*").mapPartitionsWithIndex(GetCellIdList)
print(cellIdListRDD.collect())
print(cellIdListRDD.getNumPartitions())

gridCellRSRPListRDD = sc.textFile("BCD/GridCellRSRP/*").mapPartitionsWithIndex(GetGridCellRSRP)
print(gridCellRSRPListRDD.collect())
print(gridCellRSRPListRDD.getNumPartitions())

gridListRDD = sc.textFile("BCD/GridList/*").mapPartitionsWithIndex(GetGridList)
print(gridListRDD.collect())
print(gridListRDD.getNumPartitions())

cellAntennaGainListRDD = cellListRDD.mapValues(GetCellAntennaGainList)
print(cellAntennaGainListRDD.collect())


def addDict(value, dictName):
    value[0][dictName] = value[1]
    return value[0]


dataRDD = XRangeRDD.join(cellListRDD, PARTITON_NUM).map(lambda kv: (kv[0], addDict(kv[1], 'cellList'))).join(
    cellIdListRDD, PARTITON_NUM).map(lambda kv: (kv[0], addDict(kv[1], 'cellIdList'))).join(
    gridCellRSRPListRDD, PARTITON_NUM).map(lambda kv: (kv[0], addDict(kv[1], 'gridCellRSRPList'))).join(
    gridListRDD, PARTITON_NUM).map(lambda kv: (kv[0], addDict(kv[1], 'gridList'))).join(
    cellAntennaGainListRDD, PARTITON_NUM).map(lambda kv: (kv[0], addDict(kv[1], 'cellAntennaGainList')))
print(dataRDD.collect())

gridCellLossListRDD = dataRDD.mapValues(GetGridCellLoss)
print(gridCellLossListRDD.collect())

data = dataRDD.join(gridCellLossListRDD, PARTITON_NUM).map(lambda kv: (kv[0], addDict(kv[1], 'gridCellLossList')))
print(data.collect())
print(data.getNumPartitions())
print(data.keys())
