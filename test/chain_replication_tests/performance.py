import correctness
import numpy as np
import random
import string
import sys
import time

sys.path.append('../../bin/')
import libkv739_py as kv

def genRandomString (length):
    letters = string.ascii_lowercase
    return ''. join(random.choice(letters) for _ in range (length))

def performanceTest (config_file, write_percentage = 10, db_keys = {}):

    if (len(db_keys) == 0):
        print ('db_keys is not populated. Populate db_keys')
        return None

    print ("----------- [test] Start Performance Test ------------") 
    
    percentiles = [50, 70, 90, 99]
    num_ops = len(db_keys)
    num_keys_populated = len(db_keys)
    keys_populated = list(db_keys.keys())
    num_read_failures = 0
    num_write_failures = 0
    read_times = []
    write_times = []
    
    kv.init(config_file)
    expt_start = time.time_ns()
    for i in range (num_ops):
        random_value = np.random.randint(99)
        key_number = correctness.getKeyNumber(0.1, num_keys_populated)
        key = keys_populated[key_number]
        key_len = np.random.randint(10, 2048)
        start = 0
        end = 0

        if (random_value < write_percentage):
            # Perform put operation on a random value generated on the fly
            value = genRandomString (key_len)
            start = time.time_ns()
            status, old_value = kv.put(key, value)
            end = time.time_ns()
            if (status != -1):
                time_in_us = int((end - start)/1000)
                write_times.append(time_in_us)
            else:   
                num_write_failures += 1 

        else:
            # Get the value for key
            start = time.time_ns()
            status, value = kv.get(key)
            end = time.time_ns()
            if (status != -1):
                time_in_us = int((end - start)/1000)
                read_times.append(time_in_us)
            else:
                num_read_failures += 1

    expt_end = time.time_ns()
    expt_duration_in_ms = int((expt_end - expt_start)/(1000*1000))
    throughput = (num_ops * 1000)/expt_duration_in_ms

    print ('Finished Performance Tests......')
    print (f"No. of read failures: {num_read_failures}")
    print (f"No. of write failures: {num_write_failures}")
    print (f"Percentage of writes: {write_percentage}")

    write_times.sort()
    read_times.sort()

    print ('Read times..')
    for i in percentiles:
        if len(read_times) > 0:
            read_idx = int ((i * (len(read_times)-1))/100)
            print (f"{i}th percentile read latency: {read_times[read_idx]}")
    
    print ('................................')    
    
    print ('Write times..')
    for i in percentiles:
        if len(write_times) > 0:
            write_idx = int ((i * (len(write_times) - 1))/100)
            print (f"{i}th percentile write latency: {write_times[write_idx]}")
    
    print ('................................')
    print (f"Throughput = {throughput:.2f}")
    print ("----------- [test] End Performance Test --------------") 
 
