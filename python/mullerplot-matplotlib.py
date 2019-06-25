#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Muller like plots with params values as color decoded...
"""
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

    try:
        opts, args = getopt.getopt(argv, "hi:s:p:o:", [
                        "iFile=", "sFile", "pFile", "oFile="])
    except getopt.GetoptError:
        print('test.py -i <inputFile> -s <statsFile> \
              -p <paramsFile> -o <outputFile>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('test.py -i <inputFile> outputFile -s \
                  <statsFile> -p <paramsFile> -o <outputFile>')
            sys.exit()
        elif opt in ("-i", "--iFile"):
            inputFile = arg
        elif opt in ("-s", "--sFile"):
            statsFile = arg
        elif opt in ("-p", "--pFile"):
            paramsFile = arg
        elif opt in ("-o", "--oFile"):
            outputFile = arg

    return (inputFile, statsFile, paramsFile, outputFile)


def getData(fileName):
    df = pd.read_csv(fileName, sep=";")
    return df


def buildColorsList(data, cmap):
    normalize = colors.Normalize(vmax=data.max(), vmin=data.min())
    colorList = cmap(normalize(data))

    return colorList

if __name__ == '__main__':

    inputFile, statsFile, paramsFile, outputFile = get_args(sys.argv[1:])

    data = getData(inputFile)
    statsData = getData(statsFile)
    paramsData = getData(paramsFile)
    cmap = plt.get_cmap('Blues')
    # cmap = plt.cm.get_cmap('RdYlBu')

    params = iter(paramsData.columns.values)
    next(params)
    for idx, val in enumerate(params):
        stat = paramsData[val].values
        stat = np.append(0,stat) # first row = param value for mutation_id=0
        #stat = np.column_stack((np.zeros(np.shape(stat)[0]), stat))
        clist = buildColorsList(stat, cmap)
        # cmap = colors.LinearSegmentedColormap.from_list('my_cmap',clist)

        time = data.iloc[:, 0].values
        sumAll = np.sum(data.iloc[:, 1:], axis=1)
        dataNorm = data.iloc[:, 1:].div(sumAll, axis=0).values
        fig = plt.figure(figsize=(25, 20))

        sp = plt.stackplot(time, dataNorm.T, cmap=cmap, colors=clist)
        ax1 = fig.add_subplot(111)
        ax2 = ax1.twiny()
        ax1Ticks = ax1.get_xticks()
        ax2Ticks = ax1Ticks

        def tick_function(X):
            V = X
            return ["%.3f" % z for z in V]

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
        sm = plt.cm.ScalarMappable(cmap=cmap,
                                   norm=plt.Normalize(vmin=stat.min(),
                                                      vmax=stat.max()))
        sm._A = []
        colorbar = plt.colorbar(sm)
        colorbar.set_label(val, size='xx-large')

        outputFienName = outputFile+val+'.png'
        plt.savefig(outputFienName, dpi=150)
        plt.cla()
        plt.clf()
        plt.close(fig)
        # plt.show()

print("That's all.")
