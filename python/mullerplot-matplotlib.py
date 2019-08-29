#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Muller like plots with params values as color decoded...
"""
import pandas as pd
import numpy as np
import sys
import fire
import matplotlib as mpl
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
from matplotlib import colors


def getData(fileName):
    df = pd.read_parquet(fileName)

    return df


def buildColorsList(data, cmap):
    normalize = colors.Normalize(vmax=1.0, vmin=0.0)
    colorList = cmap(normalize(data))

    return colorList


def muller_plots(input_file, stats_file, params_file, large_muller_order,
                 output_prefix):

    data = getData(input_file)
    statsData = getData(stats_file)
    paramsData = pq.read_table(params_file).flatten().to_pandas()
    order = getData(large_muller_order)
    paramsData = order.join(paramsData.set_index("mutationId"),
                            on="mutationId", how="inner")
    cmap = plt.get_cmap('nipy_spectral', data.shape[1])
    # cmap = plt.cm.get_cmap('RdYlBu')

    params = iter(paramsData.columns.values[1:,])
    next(params)
    for idx, val in enumerate(params):
        stat = paramsData[val].values
        stat = np.append(0, stat)
        # first row = param value for mutation_id=0
        # stat = np.column_stack((np.zeros(np.shape(stat)[0]), stat))
        clist = buildColorsList(stat, cmap)
        # cmap = colors.LinearSegmentedColormap.from_list('my_cmap',clist)

        time = data.iloc[:, 0].values
        sumAll = np.sum(data.iloc[:, 1:], axis=1)
        dataNorm = data.iloc[:, 1:].div(sumAll, axis=0).values
        fig = plt.figure(figsize=(25, 20))

        sp = plt.stackplot(time, dataNorm.T, cmap=cmap, colors=clist)
        ax1 = fig.add_subplot(111)
        ax1.set_xlim([0.0, time.max()])
        ax1.set_ylim([0.0, dataNorm.T.max()])
        ax2 = ax1.twiny()
        ax1Ticks = ax1.get_xticks()
        ax2Ticks = ax1Ticks
        #plt.xlim([0.0, time.max()])
        #plt.ylim([0.0, dataNorm.T.max()])

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
            'fractions of cell clones', fontsize='xx-large')
        # plt.xticks(fontsize='xx-large')
        # plt.yticks(fontsize='xx-large')
        ax1.set_xlabel('time [sytem]', fontsize='xx-large')
        ax2.set_xlabel('number of cells in system', fontsize='xx-large')
        sm = plt.cm.ScalarMappable(cmap=cmap,
                                   norm=plt.Normalize(vmin=0.0,
                                                      vmax=1.0))
        sm._A = []
        colorbar = plt.colorbar(sm)
        colorbar.set_label(val, size='xx-large')

        outputFienName = output_prefix+val+'.png'
        plt.savefig(outputFienName, bbox_inches='tight', dpi=150)

        # plt.show()
        plt.cla()
        plt.clf()
        plt.close(fig)
        

if __name__ == "__main__":
    fire.Fire(muller_plots)
    print("That's all.")
