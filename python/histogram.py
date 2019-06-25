#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Muller like plots with params values as color decoded...
"""
import numpy as np
import csv
import sys
import os
import getopt
import matplotlib.pyplot as plt


def get_args(argv):
    inputFile = ''
    statsFile = ''
    thresholdFile = ''
    outputFile = ''

    try:
        opts, args = getopt.getopt(argv, "hi:t:o:", [
                        "iFile=", "threshold", "oFile="])
    except getopt.GetoptError:
        print('test.py -i <inputFile> -t <threshold> -o <outputFile>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('test.py -i <inputFile> outputFile \
                  -t <threshold> -o <outputFile>')
            sys.exit()
        elif opt in ("-i", "--iFile"):
            inputFile = arg
        elif opt in ("-t", "--threshold"):
            threshold = arg
        elif opt in ("-o", "--oFile"):
            outputFile = arg

    return (inputFile, threshold, outputFile)

if __name__ == '__main__':

    inputFile, threshold, outputFile = get_args(sys.argv[1:])

    sortedData = os.popen('tail -n +2 '+inputFile+' | sort \
                          -t ";" -k2,2rn --stable |  \
                          head -n '+threshold).read()
    print(sortedData)
    data = csv.reader(sortedData.splitlines(), delimiter=';')
    data = list(data)
    data = np.array(data, dtype=int)

    localSum = np.sum(data.T[1][1, ])
    totalSum = data.T[1][0]
    perCent = localSum/totalSum*100

    # data = np.delete(data, 0, axis=0)
    ''' delete mutation_id=1, evry cell has this mutation '''
    labels = np.array(data).T
    index = np.arange(data.shape[0])

    fig = plt.figure(figsize=(15, 12))
    ax = fig.add_subplot(111)
    plt.bar(index, data.T[1])
    plt.xlabel("mutation_id")
    plt.ylabel("number of cells")
    plt.xticks(index, labels[0])
    plt.grid()

    textstr = '\n'.join((
                        'sum=%d' % (localSum),
                        'total nb of cells=%d' % (totalSum),
                        'percent of all cells in system=%.2f%%' % perCent))

    ax.text(x=0.75, y=0.85, s=textstr, transform=ax.transAxes)

    for idx, val in enumerate(labels[1]):
        plt.text(x=idx, y=val, s=f"{val}", fontdict=dict(fontsize=12))

    # plt.show()
    plt.savefig(outputFile)

print("That's all.")
