#!/usr/bin/env python

""" hist_mapper.py

A Hadoop mapper to calculate the histogram of a large single-column list of numbers

* Input: a list of numbers
  Example:
    1.0
    1.0
    2.0
    2.0
    3.0
* output: a list of number\t1 pairs
  Example:
    1.0\t1
    1.0\t1
    2.0\t1
    2.0\t1
    3.0\t1
"""

import sys
    

def read_input(file):
    for line in file:
        yield line.strip()


def main():
    for data in read_input(sys.stdin):
        print '%s\t1' % data


if __name__ == "__main__":
    main()
