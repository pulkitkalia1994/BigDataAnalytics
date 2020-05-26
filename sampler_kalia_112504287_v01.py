##########################################################################
## Simulator.py  v 0.1
##
## Implements two versions of a multi-level sampler:
##
## 1) Traditional 3 step process
## 2) Streaming process using hashing
##
##
## Original Code written by H. Andrew Schwartz
## for SBU's Big Data Analytics Course 
## Spring 2020
##
## Student Name: KEY
## Student ID: 

##Data Science Imports: 
import numpy as np
import mmh3
from random import random


##IO, Process Imports: 
import sys
from pprint import pprint
from datetime import datetime


##########################################################################
##########################################################################
# Task 1.A Typical non-streaming multi-level sampler

def typicalSampler(filename, percent = .01, sample_col = 0):
    # Implements the standard non-streaming sampling method
    data=set()
    # Step 1: read file to pull out unique user_ids from file
    for f in filename:
        elements=f.split(',')
        data.add(elements[sample_col])
    # Step 2: subset to random  1% of user_ids
    randoms=np.random.choice(tuple(data),int(len(data)*percent))
    randoms=set(randoms)
    # Step 3: read file again to pull out records from the 1% user_id and compute mean withdrawn
    total_sum=0
    count=0
    s=0
    mean_calculated=0.0
    standard_deviation_calculated=0.0
    filename.seek(0)

    l=[]
    for f in filename:
        elements=f.split(',')
        if(elements[sample_col] in randoms):
            l.append(float(elements[3]))
            count=count+1
    if(count!=0):
        for x in l:
            total_sum=total_sum+x
        for x in l:
            s=s+(x-(total_sum)/count)*(x-(total_sum)/count)
                
        mean_calculated=total_sum/count
        standard_deviation_calculated=np.sqrt([s/count])[0]
            
    mean, standard_deviation = mean_calculated, standard_deviation_calculated
    
    ##<<COMPLETE>>


    return mean, standard_deviation


##########################################################################
##########################################################################
# Task 1.B Streaming multi-level sampler

def streamSampler(stream, percent = .01, sample_col = 0):
    # Implements the standard streaming sampling method:
    #   stream -- iosteam object (i.e. an open file for reading)
    #   percent -- percent of sample to keep
    #   sample_col -- column number to sample over
    #
    # Rules:
    #   1) No saving rows, or user_ids outside the scope of the while loop.
    #   2) No other loops besides the while listed. 
    
    total_sum=0
    count=0
    sum_of_squares=0
    mean_calculated=0.0
    standard_deviation_calculated=0.0
    Mean=0.0
    Sum=0.0
    ##<<COMPLETE>>
    num_of_buckets=int(1/percent)
    bucket_to_choose=np.random.randint(0,num_of_buckets)
    
    if stream.name=='transactions_large.csv':
        for line in stream:
            elements=line.split(',')
            if elements:
                curr_bucket=mmh3.hash(str(elements[sample_col]), signed=False)%num_of_buckets 
                if(curr_bucket!=bucket_to_choose):
                    pass
                else:
                    count=count+1
                    oldMean = Mean
                    Mean = Mean + (float(elements[3])-Mean)/count
                    Sum = Sum + (float(elements[3])-Mean)*(float(elements[3])-oldMean)
            else:
                standard_deviation_calculated=None
                mean_calculated=None
                
    if stream.name=='transactions_large.csv':
        if (count!=0):
            standard_deviation_calculated=np.sqrt([Sum/(count-1)])[0]
            mean_calculated=Mean  
        else:
            standard_deviation_calculated=None
            mean_calculated=None
    
                
    else:   
        for line in stream:
            elements=line.split(',')
            if elements:
                curr_bucket=mmh3.hash(elements[sample_col], signed=False)%num_of_buckets 
                if(curr_bucket!=bucket_to_choose):
                    pass
                else:
                    total_sum=total_sum+float(elements[3])
                    count=count+1
                    sum_of_squares=sum_of_squares+(float(elements[3])*float(elements[3]))
            else:
                standard_deviation_calculated=None
                mean_calculated=None
    ##<<COMPLETE>>
        if(count!=0):
            mean_calculated=total_sum/count
            standard_deviation_calculated=(sum_of_squares/count) - (mean_calculated*mean_calculated)
            standard_deviation_calculated=np.sqrt([standard_deviation_calculated])[0]
        else:
            mean_calculated=None
            standard_deviation_calculated=None
    
    mean, standard_deviation = mean_calculated, standard_deviation_calculated
    
    return mean, standard_deviation


##########################################################################
##########################################################################
# Task 1.C Timing

files=['transactions_small.csv', 'transactions_medium.csv','transactions_large.csv']
percents=[.02, .005]

if __name__ == "__main__": 

    ##<<COMPLETE: EDIT AND ADD TO IT>>
    for perc in percents:
        print("\nPercentage: %.4f\n==================" % perc)
        for f in files:
            print("\nFile: ", f)
            fstream = open(f, "r")
            start=datetime.now()
            print("  Typical Sampler: ", typicalSampler(fstream, perc, 2))
            end=datetime.now()
            diff=end-start
            print("Time Taken: ",diff.total_seconds()*1000,"ms")
            fstream.close()
            fstream = open(f, "r")
            start=datetime.now()
            print("  Stream Sampler:  ", streamSampler(fstream, perc, 2))
            end=datetime.now()
            diff=end-start
            print("Time Taken: ",diff.total_seconds()*1000,"ms")
            
