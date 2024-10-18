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

def populateDB(config_file, client_id, num_keys, vk_ratio):
    print("Populating DB..........")
    total_duration = 0
    num_write_failures = 0
    keys = []
    values = []
    if (vk_ratio == 0):
        keys = ["client" + str(client_id) + "_test_key_" + str(i) for i in range (num_keys)]
        values = ["test_value_" + str(i) for i in range (num_keys)]

    kv.init(config_file)
    for i in range(num_keys):
        key = ""
        value = ""
        
        if (vk_ratio != 0):
            key_len = np.random.randint(20,max_key_length)
            key = genRandomString(key_len)
            value_len = key_len * vk_ratio
            value = genRandomString(value_len)
            keys.append(key)
            values.append(value)
        else:
            key = keys[i]
            value = values[i]
        
        start = time.time_ns()
        status, old_value = kv.put(key, value)
        end = time.time_ns()
        if (status != -1):
            time_in_us = (end - start)//1000
            total_duration += time_in_us
            # print(f"Duration: {time_in_us}")
        else:
            num_write_failures += 1
    print (f"Num put() failures: {num_write_failures}")
    print (f"Total duration: {total_duration}")
    kv.shutdown()
    print("Populating DB Completed.........")
    return keys, values
    
def performanceTest(config_file, vk_ratio, num_keys, write_percentage = 10, skew = True, keys =[], values = []):
    print ("\nRunning Performance Tests...........\n")
    percentiles = [50, 70, 90, 99]

    if len(keys) == 0:
        keys = ["client" + str(id) + "_test_key_" + str(i) for i in range (num_keys)]
    if len(values) == 0:
        values = ["test_value_" + str(i) for i in range (num_keys)]

    num_ops = 20*len(keys)
    num_keys_populated = len(keys)
    num_read_failures = 0
    num_write_failures = 0
    read_times = []
    write_times = []
    
    print (f"Num_ops = {num_ops}, num_keys_populated = {num_keys_populated}")
    kv.init(config_file)
    expt_start = time.time_ns()
    for i in range (num_ops):
        random_value = np.random.randint(99)
    
        if (skew):
            key_number = correctness.getKeyNumber(0.1, num_keys_populated)
        else:
            key_number = np.random.randint(num_keys_populated)

        key = keys[key_number]
        max_len = min(2048, 10*len(key))
        value_len = np.random.randint(len(key), max_len)
        if (vk_ratio != 0):
            value_len = len(key) * vk_ratio
        start = 0
        end = 0

        if (random_value < write_percentage):
            # Perform put operation on a random value generated on the fly
            value = genRandomString (value_len)
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
    expt_duration_in_us = int((expt_end - expt_start)/1000)
    throughput = (num_ops * 1000 * 1000)/(expt_duration_in_us)

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
    print ("\nEnding Performance Tests.................") 
