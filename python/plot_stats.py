#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import fire
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

plans = [
    {'columns': ['normalized_entropy']},
    {'columns': ['cloneCount']},
    {'columns': ['systemSize']},
    {'columns': ['entropy'], 'unit': 'bits'},
    {'columns': ['mean_birth_efficiency', 'stddev_birth_efficiency']},
    {'columns': ['mean_birth_resistance', 'stddev_birth_resistance']},
    {'columns': ['mean_lifespan_efficiency', 'stddev_lifespan_efficiency']},
    {'columns': ['mean_lifespan_resistance', 'stddev_lifespan_resistance']},
    {'columns': ['mean_success_efficiency', 'stddev_success_efficiency']},
    {'columns': ['mean_success_resistance', 'stddev_success_resistance']},
    {'columns': ['cloneCount'], 'logy':True},
    {'columns': ['systemSize'], 'logy':True}
]


def plot_stats(input_file, time_file, output_prefix):
    time = pd.read_parquet(time_file)
    data = pd.read_parquet(input_file)

    def plot_separetly(columns, logy=False, unit=None):
        fig = plt.figure()

        if logy:
            myPlot = plt.semilogy
        else:
            myPlot = plt.plot
        for column in columns:
            myPlot(data['time'], data[column], '-+', label=column)
        if unit is not None:
            plt.ylabel('[%s]' % (unit), fontsize='small')
        plt.grid(True)
        plt.xlabel('time [in system]', fontsize='small')
        # plt.ylim(0,2)
        plt.legend()
        outFileName = output_prefix + '-'.join(columns) + '.png'
        plt.savefig(outFileName, bbox_inches='tight', dpi=150)
        plt.close(fig)

    data.insert(0, "normalized_entropy",
                data['entropy']/np.log2(data['cloneCount']))
    data.insert(0, "time", time)
    for plan in plans:
        plot_separetly(**plan)


if __name__ == '__main__':

    fire.Fire(plot_stats)
    print("That's All.")
