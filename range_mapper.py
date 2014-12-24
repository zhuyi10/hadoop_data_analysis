#!/usr/bin/env python

""" range_mapper.py

A Hadoop mapper to calculate the range of a large single-column list of numbers

* Input: a list of numbers
  Example:
    1.0
    2.0
    3.0
* output: a list of 1\tnumber pairs
  Example:
    1.0\t
    2.0\t
    3.0\t
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
