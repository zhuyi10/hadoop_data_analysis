#!/usr/bin/env python

""" runhadoop.py

Run hadoop streaming to calculate the mean or the variation of a large single-column list of numbers

"""

import sys
import os
import glob
import argparse
import subprocess
import shutil

class HadoopRunner(object):
    """ Execute Map-Reduce Jobs
    
    This class execute hadoop streaming with python scripts by subprocess.check_call
    
    """


    @classmethod
    def h_range(cls, args):
        """Compute the range
        
        Procedure:
        Mapper: range_mapper.py
        Reducer: range_reducer.py
        """
        
        print '\n\n-----------------------------------------------'
        print 'Map-Reduce Job: Computing the range'
        print '-----------------------------------------------\n\n'
        subprocess.check_call([
           args.hadoophome,
           'jar', args.streaminghome,
           '-input', args.input,
           '-output', args.output,
           '-mapper', './range_mapper.py',
           '-reducer', './range_reducer.py',
        ])


    @classmethod
    def h_hist(cls, args):
        """Compute the histogram
        
        * Compute the range
        * Compute the histogram
        
        Procedure:
        Mapper1: range_mapper.py
        Reducer1: range_reducer.py
        Mapper2: hist_mapper.py
        Reducer2: hist_reducer.py
        """
        
        org_output = args.output
        args.output = './hadoop_tmp'
        cls.h_range(args)
        assert os.path.exists('./hadoop_tmp/part-00000') is True, 'Cannot get the range of the data'
        args.output = org_output
        print '\n\n-----------------------------------------------'
        print 'Map-Reduce Job: Computing the histgram'
        print '-----------------------------------------------\n\n'
        subprocess.check_call([
           args.hadoophome,
           'jar', args.streaminghome,
           '-input', args.input,
           '-output', args.output,
           '-mapper', './hist_mapper.py',
           '-reducer', './hist_reducer.py',
        ])


    @classmethod
    def h_mean(cls, args):
        """Compute the mean
        
        Procedure:
        Mapper: mean_mapper.py
        Reducer: mean_reducer.py
        """
        
        print '\n\n-----------------------------------------------'
        print 'Map-Reduce Job: Computing the mean'
        print '-----------------------------------------------\n\n'
        subprocess.check_call([
           args.hadoophome,
           'jar', args.streaminghome,
           '-input', args.input,
           '-output', args.output,
           '-mapper', './mean_mapper.py',
           '-reducer', './mean_reducer.py',
        ])


    @classmethod
    def h_var(cls, args):
        """Compute the variation
        
        Procedure:
        Mapper: var_mapper.py
        Reducer: var_reducer.py
        """
        
        print '\n\n-----------------------------------------------'
        print 'Map-Reduce Job: Computing the variation'
        print '-----------------------------------------------\n\n'
        subprocess.check_call([
           args.hadoophome,
           'jar', args.streaminghome,
           '-input', args.input,
           '-output', args.output,
           '-mapper', './var_mapper.py',
           '-reducer', './var_reducer.py',
        ])

        
    @classmethod
    def h_std(cls, args):
        """Compute the standard variation
        
        Procedure:
        Mapper: std_mapper.py
        Reducer: std_reducer.py
        """
        
        print '\n\n-----------------------------------------------'
        print 'Map-Reduce Job: Computing the standard variation'
        print '-----------------------------------------------\n\n'
        subprocess.check_call([
           args.hadoophome,
           'jar', args.streaminghome,
           '-input', args.input,
           '-output', args.output,
           '-mapper', './std_mapper.py',
           '-reducer', './std_reducer.py',
        ])


functions = {
             'range':HadoopRunner.h_range,
             'hist':HadoopRunner.h_hist,
             'mean':HadoopRunner.h_mean,
             'var':HadoopRunner.h_var,
             'std':HadoopRunner.h_std
            }        


def start():
    parser = argparse.ArgumentParser(description='Run hadoop to calcucate the mean or the variation of a large single-column list of numbers.')
    parser.add_argument('--hadoophome', required=True,
                        help="The directory of hadoop bin")
    parser.add_argument('--streaminghome', required=True,
                        help="The directory of hadoop streaming jar")
    parser.add_argument('--input', required=True, default=os.path.join('.', 'input'),
                        help="The input directory of map-reduce job")
    parser.add_argument('--output', required=True, default=os.path.join('.', 'output'),
                        help="The output directory of map-reduce job")                        
    parser.add_argument('--function', required=True, choices=functions, default='hist',
                        help="The function executed by map-reduce")                    
    args = parser.parse_args()
    
    args.hadoophome = os.path.join(os.path.expanduser(args.hadoophome), 'hadoop')
    assert os.path.exists(args.hadoophome) is True, \
           'The hadoophome does not have hadoop.'
    streams = glob.glob(os.path.join(os.path.expanduser(args.streaminghome), 'hadoop-streaming*.jar'))
    assert len(streams) > 0, \
           'The streaminghome does not have hadoop-streaming*.jar.'
    args.streaminghome = streams[0]                   
    assert os.path.isdir(os.path.expanduser(args.output)) is False, \
           'The output directory exists. Or it is not a directory.'
                         
    run(args)

    
def run(args):
    functions[args.function](args)
    

if __name__ == "__main__":
    try:
        start()
    except KeyboardInterrupt:
        pass
    except IOError as e:
        if e.errno == errno.EPIPE:
            pass
        else:
            raise
