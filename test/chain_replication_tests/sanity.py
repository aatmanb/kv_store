import numpy as np
import time
import sys
import pandas as pd
import string
import random

sys.path.append('../../bin/')
import libkv739_py as kv

vk_ratio = 16

def generate_string(n):
    return ''.join(random.choice(string.ascii_letters) for _ in range(n))

def runGetTest(config_file, id, num_keys, log_dir, key_length, value_length):
    print("Running sanity GetTest")
    latency_arr = np.array([])
    start_time_arr = np.array([])
    throughput_arr = np.array([], dtype=float)
    key = ""
    value = ""
    total_duration = 0
    throughput_calculation_interval_threshold = 2000.0 # milliseconds
    throughput_calculation_interval = 0
    num_keys_per_throughput_calculation_interval = 0.0
    kv.init(id, config_file, log_dir)
    test_start = time.time_ns()
    for i in range(num_keys):
        #key = "test_key_" + 
        key = generate_string(key_length)
        #key += str(i)
        start = time.time_ns()
        status, value = kv.get(key)
        end = time.time_ns()
        
        latency = (end - start)/(1000 * 1000)
        total_duration += latency
        test_duration = start - test_start
        num_keys_per_throughput_calculation_interval += 1.0
        throughput_calculation_interval += latency
        if (throughput_calculation_interval > throughput_calculation_interval_threshold):
            # number of keys sent during the current interval
            throughput = (float)(num_keys_per_throughput_calculation_interval / throughput_calculation_interval_threshold) * 1000.0
            num_keys_per_throughput_calculation_interval = 0
            throughput_calculation_interval = 0
            throughput_arr = np.append(throughput_arr, throughput)
        #print(f"duration: {latency}")
        latency_arr = np.append(latency_arr, int(latency))
        start_time_arr = np.append(start_time_arr, int(test_duration/(1000*1000)))
    avg_latency = np.mean(latency_arr)
    avg_throughput = np.mean(throughput_arr)
    print(f"key_length: {key_length}")
    print(f"value_length: {value_length}")
    print (f"Total duration: {total_duration}")
    print (f"avg_latency: {avg_latency}")
    print (f"avg_throughput: {avg_throughput}")

    #file_prefix = f"/client_{id}_sanity_get_k{key_length}_v{value_length}"
    file_prefix = f"/client_{id}_sanity_get"

    # Latency
    df = pd.DataFrame({'latency': latency_arr, 'time': start_time_arr})
    filename = log_dir + file_prefix + "_latency.csv"
    df.to_csv(filename)
    
    # Throughput
    df = pd.DataFrame({'throughput': throughput_arr})
    filename = log_dir + file_prefix + "_throughput.csv"
    df.to_csv(filename)
    kv.shutdown()
    print("GetTest completed")

def runPutTest(config_file, id, num_keys, log_dir, key_length, value_length):
    print("Running sanity PutTest")
    latency_arr = np.array([])
    start_time_arr = np.array([])
    throughput_arr = np.array([], dtype=float)
    key = ""
    value = ""
    old_value = ""
    total_duration = 0
    throughput_calculation_interval_threshold = 2000 # milliseconds
    throughput_calculation_interval = 0
    num_keys_per_throughput_calculation_interval = 0.0
    kv.init(id, config_file, log_dir)
    test_start = time.time_ns()
    for i in range(num_keys):
        #key = "test_key_"
        key = generate_string(key_length)
        #key += str(i)
        #value = "test_value_" 
        value = generate_string(value_length)
        value += str(i) 
        start = time.time_ns()
        status, value = kv.put(key, value)
        end = time.time_ns()
        
        latency = (end - start)/(1000 * 1000)
        total_duration += latency
        test_duration = start - test_start
        num_keys_per_throughput_calculation_interval += 1.0
        throughput_calculation_interval += latency
        if (throughput_calculation_interval > throughput_calculation_interval_threshold):
            # number of keys sent during the current interval
            throughput = (float)(num_keys_per_throughput_calculation_interval / throughput_calculation_interval) * 1000.0
            print(num_keys_per_throughput_calculation_interval)
            print(throughput_calculation_interval)
            print(throughput)
            num_keys_per_throughput_calculation_interval = 0
            throughput_calculation_interval = 0
            throughput_arr = np.append(throughput_arr, throughput)
        #print(f"duration: {latency}")
        latency_arr = np.append(latency_arr, int(latency))
        start_time_arr = np.append(start_time_arr, int(test_duration/(1000*1000)))
    avg_latency = np.mean(latency_arr)
    avg_throughput = np.mean(throughput_arr)
    print(f"key_length: {key_length}")
    print(f"value_length: {value_length}")
    print (f"Total duration: {total_duration}")
    print (f"avg_latency: {avg_latency}")
    print (f"avg_throughput: {avg_throughput}")
    
    #file_prefix = f"/client_{id}_sanity_put_k{key_length}_v{value_length}"
    file_prefix = f"/client_{id}_sanity_put"
    
    # Latency
    df = pd.DataFrame({'latency': latency_arr, 'time': start_time_arr})
    filename = log_dir + file_prefix + "_latency.csv"
    df.to_csv(filename)
    
    # Throughput
    df = pd.DataFrame({'throughput': throughput_arr})
    filename = log_dir + file_prefix + "_throughput.csv"
    df.to_csv(filename)
    kv.shutdown()
    print("PutTest completed")

def runSanityTest(config_file, id, num_keys, log_dir):
    print("Running sanity tests")
    for i in range(4, 5, 1):
        key_length = 2 ** i
        value_length = int(vk_ratio * key_length)
        #runPutTest(config_file, id, num_keys, log_dir, key_length, value_length)
        runGetTest(config_file, id, num_keys, log_dir, key_length, value_length)
    print("Sanity tests completed")
