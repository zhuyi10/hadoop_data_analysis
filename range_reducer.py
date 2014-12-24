#!/usr/bin/env python

""" range_reducer.py

A Hadoop reducer to calculate the histogram of a large single-column list of numbers
Corresponding mapper range_mapper.py

* Input: a list of number\t1 pairs
  Example:
    1.0\t
    2.0\t
    3.0\t
* output: the histogram
  Example:
    1.0\t2.0\t3
"""

import sys, math, shutil

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.strip().split(separator)

def main():
    h_max = h_mun = 0
    h_min = None
    for data in read_mapper_output(sys.stdin):
        if h_min is None: h_min = float(data[0])
        h_max = float(data[0])
        h_mun += 1
    print "%f\t%f\t%d" %(h_min, h_max, h_mun)

if __name__ == "__main__":
    main() 
