import csv
import json
import math
import os
import requests
import time
from pathlib import Path
from config import config


def fetchData(chain, subgraphURL, pool, startTime=1652000000, endTime=1652089128, force=True):

    columns = """{
    amount0
    amount1
    timestamp
    origin
    sqrtPriceX96
    tick
    logIndex
    transaction {id\\n blockNumber\\n gasUsed\\n gasPrice}
}""".split("\n")
    columns = "\\n".join(columns)

    cousorTime = startTime
    timeStep = 86400
    scriptDir = Path(__file__).parent.absolute()
    rawDataFilePath = f'{scriptDir}//data//{chain}_swap.txt'
    if force:
        if os.path.exists(rawDataFilePath):
            os.remove(rawDataFilePath)
    else:
        assert os.path.exists(
            rawDataFilePath), f'{rawDataFilePath} already existing.'
    f = open(rawDataFilePath, "a")
    totalCount = 0
    while cousorTime < endTime:
        dpayload = '{"query":"{\\n swaps(first: 1000, orderBy: timestamp, orderDirection: asc, where:' + \
            '{ pool: \\"' + pool + '\\", '
        dpayload += 'timestamp_lte: ' + str(cousorTime+timeStep)
        dpayload += ', timestamp_gt: ' + str(cousorTime) + '}) '
        dpayload += columns + '\\n}\\n","variables":null}'
        data = dpayload

        headers = {
            'cache-control': 'no-cache',
            'content-type': 'application/json'
        }
        response = requests.post(
            subgraphURL, headers=headers, data=data)
        jdata = response.json()

        tmpCousorTime = cousorTime
        if response.status_code == 200:
            try:
                for o in jdata["data"]["swaps"]:
                    dataTimestamp = int(o["timestamp"])
                    tmpCousorTime = dataTimestamp
                    totalCount += 1
                    f.write(json.dumps(o)+"\n")
                cousorTime = tmpCousorTime
                print(tmpCousorTime)
            except:
                time.sleep(10)
        else:
            print(
                f'${cousorTime} response.status_code: ${response.status_code}, will retry')
    f.close()
    print(f'total counts: {totalCount}')


def Data2CSV(chain):

    scriptDir = Path(__file__).parent.absolute()
    file1 = open(f'{scriptDir}//data//{chain}_swap.txt', 'r')
    lines = file1.readlines()
    # print(lines)
    if len(lines) != 0:
        fileOutputDir = f'{scriptDir}'
        if not os.path.exists(fileOutputDir):
            os.mkdir(fileOutputDir)
        fileOutputFileth = f'{fileOutputDir}//data//{chain}_swap.csv'
        if os.path.exists(fileOutputFileth):
            os.remove(fileOutputFileth)
        f = open(fileOutputFileth, 'w')
        cwriter = csv.writer(f)
        header = ["pool", "block_number", "block_hash",
                  "block_time", "log_index", "tx_hash",
                  "sender", "recipient", "amount0",
                  "amount1", "sqrt_price_x96", "liquidity",
                  "price", "tick", "gasUsed", "gasPrice"]
        cwriter.writerow(header)

        blockNumber = 0
        for line in lines:
            o = json.loads(line)
            sqrtPriceX96 = int(o["sqrtPriceX96"])

            price = (2 ** 192) / ((sqrtPriceX96 / 10 ** 6) ** 2)
            row = [
                # "pool", "block_number"
                "mypool", o['transaction']['blockNumber'],
                # "block_hash", "block_time"
                o['transaction']["id"], o["timestamp"],
                # "log_index", "tx_hash"
                int(o["logIndex"]), o['transaction']["id"],
                # "sender", "recipient"
                o["origin"], "recipient",
                # "amount0", "amount1
                (-1) * float(o["amount0"]), (-1) * float(o["amount1"]),
                # sqrt_price_x96
                int(o["sqrtPriceX96"]),
                # "liquidity", "price"
                0, price,
                # "tick"
                o['tick'],
                # "gasUsed", "gasPrice"
                o['transaction']['gasUsed'], o['transaction']['gasPrice']
            ]
            blockNumber += 1
            cwriter.writerow(row)


def fetchAddLiquidity(chain, subgraphURL, pool, startTime=1659283200, endTime=1660060800, force=True):

    columns = """{
    amount0
    amount1
    timestamp
    sender
    origin 
    tickLower
    tickUpper
    logIndex
    amountUSD
    transaction {id\\n blockNumber\\n gasUsed\\n gasPrice}
}""".split("\n")
    columns = "\\n".join(columns)

    cousorTime = startTime
    timeStep = 86400
    scriptDir = Path(__file__).parent.absolute()
    rawDataFilePath = f'{scriptDir}//data//{chain}_addLiquidity.txt'
    if force:
        if os.path.exists(rawDataFilePath):
            os.remove(rawDataFilePath)
    else:
        assert os.path.exists(
            rawDataFilePath), f'{rawDataFilePath} already existing.'
    f = open(rawDataFilePath, "a")
    totalCount = 0
    while cousorTime < endTime:
        dpayload = '{"query":"{\\n mints(first: 1000, orderBy: timestamp, orderDirection: asc, where:' + \
            '{ pool: \\"' + pool + '\\", '
        dpayload += 'timestamp_lte: ' + str(cousorTime+timeStep)
        dpayload += ', timestamp_gt: ' + str(cousorTime) + '}) '
        dpayload += columns + '\\n}\\n","variables":null}'
        data = dpayload

        headers = {
            'cache-control': 'no-cache',
            'content-type': 'application/json'
        }
        response = requests.post(
            subgraphURL, headers=headers, data=data)
        jdata = response.json()

        tmpCousorTime = cousorTime
        if response.status_code == 200:
            for o in jdata["data"]["mints"]:
                dataTimestamp = int(o["timestamp"])
                tmpCousorTime = dataTimestamp
                totalCount += 1
                f.write(json.dumps(o)+"\n")
            cousorTime = tmpCousorTime
            print(tmpCousorTime)
        else:
            print(
                f'${cousorTime} response.status_code: ${response.status_code}, will retry')
    f.close()
    print(f'total counts: {totalCount}')


def AddLiquidity2CSV(chain, switch=False):
    scriptDir = Path(__file__).parent.absolute()
    file1 = open(f'{scriptDir}//data//{chain}_addLiquidity.txt', 'r')
    lines = file1.readlines()
    if len(lines) != 0:
        fileOutputDir = f'{scriptDir}'
        if not os.path.exists(fileOutputDir):
            os.mkdir(fileOutputDir)
        fileOutputFileth = f'{fileOutputDir}//data//{chain}_addLiquidity.csv'
        if os.path.exists(fileOutputFileth):
            os.remove(fileOutputFileth)
        f = open(fileOutputFileth, 'w')
        cwriter = csv.writer(f)
        header = ["timestamp", "sender", "origin", "amount0", "amount1", "amountUSD",
                  "tickLower", "tickUpper", "logIndex", "tx_hash", "block_number", "gasUsed", "gasPrice"]
        cwriter.writerow(header)

        blockNumber = 0
        for line in lines:
            o = json.loads(line)
            row = [
                o["timestamp"],
                o["sender"],
                o["origin"],
                o["amount0"],
                o["amount1"],
                o["amountUSD"],
                o["tickLower"],
                o["tickUpper"],
                o["logIndex"],
                o["transaction"]["id"],
                o["transaction"]["blockNumber"],
                o["transaction"]["gasUsed"],
                o["transaction"]["gasPrice"]
            ]
            blockNumber += 1
            cwriter.writerow(row)


def fetchRemoveLiquidity(chain, subgraphURL, pool, startTime=1659283200, endTime=1660060800, force=True):

    columns = """{
    amount0
    amount1
    timestamp
    owner
    origin 
    tickLower
    tickUpper
    logIndex
    amountUSD
    transaction {id\\n blockNumber\\n gasUsed\\n gasPrice}
}""".split("\n")
    columns = "\\n".join(columns)

    cousorTime = startTime
    timeStep = 86400
    scriptDir = Path(__file__).parent.absolute()
    rawDataFilePath = f'{scriptDir}//data//{chain}_removeLiquidity.txt'
    if force:
        if os.path.exists(rawDataFilePath):
            os.remove(rawDataFilePath)
    else:
        assert os.path.exists(
            rawDataFilePath), f'{rawDataFilePath} already existing.'
    f = open(rawDataFilePath, "a")
    totalCount = 0
    while cousorTime < endTime:
        dpayload = '{"query":"{\\n burns(first: 1000, orderBy: timestamp, orderDirection: asc, where:' + \
            '{ pool: \\"' + pool + '\\", '
        dpayload += 'timestamp_lte: ' + str(cousorTime+timeStep)
        dpayload += ', timestamp_gt: ' + str(cousorTime) + '}) '
        dpayload += columns + '\\n}\\n","variables":null}'
        data = dpayload

        headers = {
            'cache-control': 'no-cache',
            'content-type': 'application/json'
        }
        response = requests.post(
            subgraphURL, headers=headers, data=data)
        jdata = response.json()

        tmpCousorTime = cousorTime
        if response.status_code == 200:
            for o in jdata["data"]["burns"]:
                dataTimestamp = int(o["timestamp"])
                tmpCousorTime = dataTimestamp
                totalCount += 1
                f.write(json.dumps(o)+"\n")
            cousorTime = tmpCousorTime
            print(tmpCousorTime)
        else:
            print(
                f'${cousorTime} response.status_code: ${response.status_code}, will retry')
    f.close()
    print(f'total counts: {totalCount}')


def RemoveLiquidity2CSV(chain, switch=False):
    scriptDir = Path(__file__).parent.absolute()
    file1 = open(f'{scriptDir}//data//{chain}_removeLiquidity.txt', 'r')
    lines = file1.readlines()
    if len(lines) != 0:
        fileOutputDir = f'{scriptDir}'
        if not os.path.exists(fileOutputDir):
            os.mkdir(fileOutputDir)
        fileOutputFileth = f'{fileOutputDir}//data//{chain}_removeLiquidity.csv'
        if os.path.exists(fileOutputFileth):
            os.remove(fileOutputFileth)
        f = open(fileOutputFileth, 'w')
        cwriter = csv.writer(f)
        header = ["timestamp", "sender", "origin", "amount0", "amount1", "amountUSD",
                  "tickLower", "tickUpper", "logIndex", "tx_hash", "block_number", "gasUsed", "gasPrice"]
        cwriter.writerow(header)

        blockNumber = 0
        for line in lines:
            o = json.loads(line)
            row = [
                o["timestamp"],
                o["owner"],
                o["origin"],
                o["amount0"],
                o["amount1"],
                o["amountUSD"],
                o["tickLower"], o["tickUpper"],
                o["logIndex"],
                o["transaction"]["id"],
                o["transaction"]["blockNumber"],
                o["transaction"]["gasUsed"],
                o["transaction"]["gasPrice"]
            ]
            blockNumber += 1
            cwriter.writerow(row)

# for ethereum
chain = config['chain']
subgraphURL = config['subgraphURL']
pool = config['pool']

def main():
    endTime = int(time.time()) -3600  # current timestamp
    startTime = endTime - 86400 * 180  # start timestamp, default 180 days
    chain = config['chain']
    subgraphURL = config['subgraphURL']
    pool = config['pool']

    # get Data
    fetchData(chain, subgraphURL, pool, startTime, endTime)
    Data2CSV(chain)
    fetchAddLiquidity(chain, subgraphURL, pool, startTime, endTime)
    AddLiquidity2CSV(chain)
    fetchRemoveLiquidity(chain, subgraphURL, pool, startTime, endTime)
    RemoveLiquidity2CSV(chain)


main()
