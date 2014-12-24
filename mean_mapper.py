#!/usr/bin/env python

""" mean_mapper.py

A Hadoop mapper to calculate the mean of a large single-column list of numbers

* Input: a list of numbers
  Example:
    1.0
    2.0
    3.0
* output: a list of 1\tnumber pairs
  Example:
    1\t1.0
    1\t2.0
    1\t3.0
"""

import sys

def read_input(file):
    for line in file:
        yield line.strip()

def main():
    for data in read_input(sys.stdin):
        print '1\t%s' % data

if __name__ == "__main__":
    main()
