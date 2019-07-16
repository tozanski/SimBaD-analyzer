#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Muller like plots with params values as color decoded...
"""
import numpy as np
import matplotlib.pyplot as plt
import fire
import pyarrow.parquet as pq


def histogram_plot(input_file, threshold, output_file):
    data = pq.read_table(input_file,
                         columns=['mutationId', 'mutationCount']).to_pandas()
    sortedData = data.sort_values(by=['mutationCount'], ascending=False)
    sortedData = sortedData.iloc[:threshold, :threshold]
    data = np.array(sortedData)

    # data = np.delete(data, 0, axis=0)
    ''' delete mutation_id=1, evry cell has this mutation '''

    localSum = np.sum(data.T[1][1, ])
    totalSum = data.T[1][0]
    perCent = localSum/totalSum*100

    labels = np.array(data).T
    index = np.arange(data.shape[0])

    fig = plt.figure(figsize=(15, 12))
    ax = fig.add_subplot(111)
    plt.bar(index, data.T[1])
    plt.xlabel("mutation_id")
    plt.ylabel("number of cells")
    plt.xticks(index, labels[0])
    plt.grid()

    for idx, val in enumerate(labels[1]):
        plt.text(x=idx, y=val, s=f"{val}", fontdict=dict(fontsize=12))

    # plt.show()
    plt.savefig(output_file, bbox_inches='tight', dpi=150)

if __name__ == '__main__':
    fire.Fire(histogram_plot)
    print("That's all.")
