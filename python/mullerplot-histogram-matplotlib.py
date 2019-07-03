#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import tqdm
import csv
import sys
import getopt
import matplotlib as mpl
import matplotlib.pyplot as plt
from itertools import dropwhile, takewhile
from matplotlib import colors


def get_args(argv):
    inputFile = ''
    statsFile = ''
    paramsFile = ''
    outputFile = ''
    timefile = ''

    try:
        opts, args = getopt.getopt(argv, "hi:t:s:p:o:", [
                        "iFile=", "tFile", "sFile", "pFile", "oFile="])
    except getopt.GetoptError:
        print('test.py -i <inputFile> -t <timeFile> -s <statsFile> \
              -p <paramsFile> -o <outputFile>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('test.py -i <inputFile> -t <timeFile> -s \
                  <statsFile> -p <paramsFile> -o <outputFile>')
            sys.exit()
        elif opt in ("-i", "--iFile"):
            inputFile = arg
        elif opt in ("-t", "--tFile"):
            timeFile = arg
        elif opt in ("-s", "--sFile"):
            statsFile = arg
        elif opt in ("-p", "--pFile"):
            paramsFile = arg
        elif opt in ("-o", "--oFile"):
            outputFile = arg

    return (inputFile, timeFile, statsFile, paramsFile, outputFile)


def getData(fileName, header=0):
    df = pd.read_csv(fileName, sep=";", header=header)
    return df


if __name__ == '__main__':

    inputFile, timeFile, statsFile, paramsFile, outputFile \
        = get_args(sys.argv[1:])

    data = getData(inputFile, header=None)
    time = getData(timeFile)
    statsData = getData(statsFile)
    paramsData = getData(paramsFile)

    time = time.iloc[0:, 0].values
    sumAll = np.sum(data.iloc[:, :], axis=1)
    dataNorm = data.iloc[:, :].div(sumAll, axis=0).values

    fig = plt.figure(figsize=(25, 20))
    plt.stackplot(time, dataNorm.T)

    ax1 = fig.add_subplot(111)
    ax2 = ax1.twiny()
    ax1Ticks = ax1.get_xticks()
    ax2Ticks = ax1Ticks

    ax2.set_xticks(ax2Ticks)
    ax2.set_xbound(ax1.get_xbound())
    ax2.set_xticklabels(np.take(statsData['systemSize'].values,
                        ax2Ticks[0:-1].astype(int), axis=0))
    # plt.xlabel('time [sytem]', fontsize='xx-large')
    ax1.set_ylabel(
        'fractions of cell clons', fontsize='xx-large')
    # plt.xticks(fontsize='xx-large')
    # plt.yticks(fontsize='xx-large')
    ax1.set_xlabel('time [sytem]', fontsize='xx-large')
    ax2.set_xlabel('number of cells in system', fontsize='xx-large')

    outputFienName = outputFile+'.png'
    plt.savefig(outputFienName, dpi=150)
    # plt.show()
    plt.cla()
    plt.clf()
    plt.close('all')

print("That's all.")
