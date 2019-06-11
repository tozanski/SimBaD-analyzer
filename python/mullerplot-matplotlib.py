#!/usr/bin/env python3
# -*- coding: utf-8 -*-

""" Plot number of cells with particular mutation in time...
"""
import numpy as np
import tqdm
import csv
import sys
import getopt
import matplotlib.pyplot as plt
from itertools import dropwhile, takewhile

def get_args(argv):
    inputfile = ''
    outputfile = ''
    tinputfile = ''

    try:
        opts, args = getopt.getopt(argv, "hi:t:o:", [
                        "ifile=", "tfile", "ofile="])
    except getopt.GetoptError:
        print('test.py -i <inputfile> -t <tinputfile> -o <outputfile>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('test.py -i <inputfile> -t <tinputfile> -o <outputfile>')
            sys.exit()
        elif opt in ("-i", "--ifile"):
            inputfile = arg
        elif opt in ("-t", "--tfile"):
            tinputfile = arg
        elif opt in ("-o", "--ofile"):
            outputfile = arg

    return inputfile, tinputfile, outputfile



def getData(fileName):
    with open(fileName, "r") as csvfile:
        datareader = csv.reader(csvfile, delimiter=';')
        yield from takewhile(
            lambda x: x != 0, dropwhile(
                lambda x: x[0] == 'mutationId', datareader))
        return


def generatePaths(rows):
    mut_id = 0
    # current_path = [[0, 0, 1]]
    current_path = []
    for row in rows:
        if int(row[0]) != mut_id:  # new path begins here
            yield mut_id, np.array(current_path)[:, 1:]
            current_path = []
        current_path.append([int(row[0]), float(row[1]), float(row[2])])
        mut_id = int(row[0])
    yield mut_id, np.array(current_path)[:, 1:]


def generatePlots(paths, max_time):
    maximas = np.zeros(max_time)
    for mut_id, path in paths:
        x1 = path[0, 0]
        x2 = path[-1, 0]

        orig_path = path

        maximas[int(x1): int(x2)+1] = orig_path[:, 1]
        yield mut_id, maximas.copy()


def get_max_time(paths):
    m = 0.0
    for mutId, path in generatePaths(tqdm.tqdm(getData(iFileName))):
        m = max( m, max(path[:,0]) )
        return m

if __name__ == '__main__':

    iFileName, tFileName, oFileName = get_args(sys.argv[1:])

    lines = []
    max_time = get_max_time(generatePaths(tqdm.tqdm(getData(iFileName))))
    print("max time=", max_time)
    for mut_id, plot_line in generatePlots(
            generatePaths(tqdm.tqdm(getData(iFileName))), int(max_time+1)):
        lines.append(plot_line)

    time = np.loadtxt(
        tFileName, delimiter=';', usecols=(0, 1), skiprows=1, unpack=True)
    # data = np.array(lines, dtype=float)
    mylines = [lines[0]] + [p[0] - p[1] for p in zip(lines[1:], lines)]
    fig = plt.figure(figsize=(25, 20))
    _ = plt.stackplot(np.array(range(len(mylines[0]))), mylines[:])
    ax1 = fig.add_subplot(111)
    ax2 = ax1.twiny()
   
    ax1Ticks = ax1.get_xticks()   
    ax2Ticks = ax1Ticks

    def tick_function(X):
        V = X
        return ["%.3f" % z for z in V]

    ax2.set_xticks(ax2Ticks)
    ax2.set_xbound(ax1.get_xbound())
    # ax2.set_xticklabels(tick_function(ax2Ticks))
    ax2.set_xticklabels(np.take(
        time.T, ax2Ticks[0:-1-1].astype(int), axis=0)[:, 1].astype(int))


    # plt.xlabel('time [sytem]', fontsize='xx-large')
    ax1.set_ylabel(
        'fractions of cell clons', fontsize='xx-large')
    plt.xticks(fontsize='xx-large')
    plt.yticks(fontsize='xx-large')
    ax1.set_xlabel('time [sytem]', fontsize='xx-large')
    ax2.set_xlabel('number of cells in system', fontsize='xx-large')

    plt.savefig(oFileName, dpi=600)
   
    
    
    #plt.figure(figsize=(20,20))
    #plt.plot(np.array(range(data.T.shape[0])), data.T)

print("That's all.")





