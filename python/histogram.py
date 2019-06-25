#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Muller like plots with params values as color decoded...
"""
import pandas as pd
import numpy as np
import tqdm
import csv
import sys, os
import getopt
import matplotlib as mpl
import matplotlib.pyplot as plt
from itertools import dropwhile, takewhile
from matplotlib import colors


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
            print('test.py -i <inputFile> outputFile -t <threshold> -o <outputFile>')
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
    
    sortedData = os.popen('tail -n +2 '+inputFile+' | sort -t ";" -k2,2rn --stable | head -n '+threshold).read()
    print(sortedData)
    data = csv.reader(sortedData.splitlines(), delimiter=';')
    data = list(data)
    data = np.array(data, dtype=int)
    labels = np.array(data).T
    index = np.arange(data.shape[0])

    fig = plt.figure(figsize=(25, 20))
    plt.bar(index, data.T[1])
    plt.xlabel("mutation_id")
    plt.ylabel("number of cells")
    plt.xticks(index, labels[0])
    plt.grid()
    #plt.yticks(data.T[1], labels[1])

    for idx,val in enumerate(labels[1]):
        plt.text(x=idx, y=val , s=f"{val}" , fontdict=dict(fontsize=12))

    #plt.show()
    plt.savefig(outputFile)

print("That's all.")
