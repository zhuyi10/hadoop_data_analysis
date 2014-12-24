#!/usr/bin/env python

""" hist_reducer.py

A Hadoop reducer to calculate the histogram of a large single-column list of numbers
Corresponding mapper hist_mapper.py

* Input: a list of number\t1 pairs
  Example:
    1.0\t1
    1.0\t1
    2.0\t1
    2.0\t1
    3.0\t1
* output: the histogram
  Example:
    1.0\t2
    2.0\t2
    3.0\t1
"""

import sys, math, shutil, os


def read_range():
    with open('./hadoop_tmp/part-00000') as f:
        d = f.readline()
        h_min, h_max, h_num = d.strip().split('\t')
        h_min = float(h_min)
        h_max = float(h_max)
        h_num = float(h_num)
    bins = math.ceil(1 + math.log(h_num)/0.693)
    w = (h_max - h_min) / bins
    return h_min , h_max , h_num , bins , w
    

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.strip().split(separator)


def main():
    h_min , h_max , h_num , bins , w = read_range()
    bins = int(bins)
    fre = []
    for i in xrange(bins):
        fre.append([h_min + (i+1)*w, 1])
    for data in read_mapper_output(sys.stdin):
        d = float(data[0])
        for i in xrange(len(fre)):
            if d <= fre[i][0]: 
                fre[i][1] += 1
                break
    for i in fre:
        print '(%f,%f)\t%d' % (i[0]-w, i[0], i[1])
    if os.path.exists('./hadoop_tmp'): shutil.rmtree('./hadoop_tmp')

if __name__ == "__main__":
    main() 
