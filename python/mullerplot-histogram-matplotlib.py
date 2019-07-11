#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import sys
import fire
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import colors

    def print_help():
        print('test.py -i <inputFile> -t <timeFile> -s <statsFile> \
              -n <parameterName> -o <outputFile>')

        elif opt in ("-n", "--pName"):
            paramName = arg
def buildColorsList(data, cmap):
    normalize = colors.Normalize(vmax=data.max(), vmin=data.min())
    colorList = cmap(normalize(data))
    return colorList

def histogram_plots(input_csv, time_parquet, stats_parquet, output_file):
    
    data = pd.read_csv(input_csv, sep=";", header=None)
    time = pd.read_parquet(time_parquet)
    statsData = pd.read_parquet(stats_parquet)

    time = time.iloc[0:, 0].values
    time = np.append([0], time, axis=0)
    sumAll = np.sum(data.iloc[:, :], axis=1)
    dataNorm = data.iloc[:, :].div(sumAll, axis=0).values

    # cmap = plt.get_cmap('Blues', data.shape[1])
    # cmap = plt.cm.get_cmap('magma_r', data.shape[1])
    cmap = plt.cm.get_cmap('nipy_spectral', data.shape[1])
    # cmap = plt.cm.get_cmap('viridis', data.shape[1])
    # https://cran.r-project.org/web/packages/viridis/vignettes/intro-to-viridis.html#gallery


    # colors = cmap.colors
    # https://matplotlib.org/3.1.0/tutorials/colors/colormap-manipulation.html
    colorsIds = np.arange(0, data.shape[1], 1)
    colors = buildColorsList(colorsIds, cmap)
    fig = plt.figure(figsize=(25, 20))
    sp = plt.stackplot(time, dataNorm.T, edgecolor='white', colors=colors)

    ax1 = fig.add_subplot(111)
    ax2 = ax1.twiny()
    ax1Ticks = ax1.get_xticks()
    ax2Ticks = ax1Ticks

    def tick_function(X):
        V = X
        return ["%.3f" % z for z in V]

    ax2.set_xticks(ax2Ticks)
    ax2.set_xbound(ax1.get_xbound())
    ticks = ax2Ticks[(np.where(ax2Ticks.astype(int) <
                     statsData['systemSize'].values.shape[0]))]
    ax2.set_xticklabels(np.take(statsData['systemSize'].values,
                        ticks.astype(int), axis=0))
    # plt.xlabel('time [sytem]', fontsize='xx-large')
    ax1.set_ylabel(
        'fractions of cell clons', fontsize='xx-large')
    # plt.xticks(fontsize='xx-large')
    # plt.yticks(fontsize='xx-large')
    ax1.set_xlabel('time [sytem]', fontsize='xx-large')
    ax2.set_xlabel('number of cells in system', fontsize='xx-large')

    sm = plt.cm.ScalarMappable(cmap=cmap, 
                               norm=plt.Normalize(vmin=dataNorm.min(),
                                                  vmax=dataNorm.max()))
    sm._A = []
    colorbar = plt.colorbar(sm)
    colorbar.set_label(paramName, size='xx-large')

    outputFileName = output_file
    plt.savefig(outputFileName, dpi=150)
    #plt.show()
    plt.cla()
    plt.clf()
    plt.close('all')

if __name__ == '__main__':
    fire.Fire(histogram_plots)
    print("That's all.")
