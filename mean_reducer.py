#!/usr/bin/env python

""" mean_reducer.py

A Hadoop reducer to calculate the mean of a large single-column list of numbers
Corresponding mapper mean_mapper.py

* Input: a list of 1\tnumber pairs
  Example:
    1\t1.0
    1\t2.0
    1\t3.0
* output: the mean
  Example:
    mean\t2.0
"""

import sys

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.strip().split(separator)[1]

def main():
    s = num = 0
    for data in read_mapper_output(sys.stdin):
        s += float(data)
        num += 1
    print 'mean\t' + str(s/num)

if __name__ == "__main__":
    main() 
