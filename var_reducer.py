#!/usr/bin/env python

""" var_reducer.py

A Hadoop reducer to calculate the variation of a large single-column list of numbers
Corresponding mapper var_mapper.py

* Input: a list of 1\tnumber\tsquare_of_number pairs
  Example:
    1\t1.0\t1.0
    1\t2.0\t4.0
    1\t3.0\t9.0
* output: the variation
  Example:
    variation\t0.67
"""

import sys

def read_mapper_output(file, separator='\t'):
    for line in file:
        yield line.strip().split(separator)

def main():
    s = ss = num = 0
    for data in read_mapper_output(sys.stdin):
        ss += float(data[2])
        s += float(data[1])
        num += 1
    print 'variation\t' + str(ss/num - (s/num)*(s/num))

if __name__ == "__main__":
    main() 
