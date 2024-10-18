import numpy as np
import time
import sys

sys.path.append('../../bin/')
import libkv739_py as kv

def runGetTest(config_file, id, num_keys):
    print("Running sanity GetTest")
    key = ""
    value = ""
    total_duration = 0
    kv.init(config_file)
    for i in range(num_keys):
        key = "test_key_" + str(i)
        start = time.time_ns()
        status, value = kv.get(key)
        end = time.time_ns()
        time_in_us = (end - start)//1000
        total_duration += time_in_us
        #print(f"[client {id}]")
        #print(f"key: {key}")
        #print(f"value: {value}")
        #print(f"status: {status}")
        print(f"duration: {time_in_us}")
        #print("\n")
    print (f"Total duration: {total_duration}")
    kv.shutdown()
    print("GetTest completed")

def runPutTest(config_file, id, num_keys):
    print("Running sanity PutTest")
    key = ""
    value = ""
    old_value = ""
    total_duration = 0
    kv.init(config_file)
    for i in range(num_keys):
        key = "test_key_" + str(i)
        value = "test_value_" + str(i) 
        start = time.time_ns()
        status, value = kv.put(key, value)
        end = time.time_ns()
        time_in_us = (end - start)//1000
        total_duration += time_in_us
        #print(f"[client {id}]")
        #print(f"key: {key}")
        #print(f"value: {value}")
        #print(f"old_value: {old_value}")
        #print(f"status: {status}")
        print(f"duration: {time_in_us}")
        #print("\n")
    print (f"Total duration: {total_duration}")
    kv.shutdown()
    print("PutTest completed")

def runSanityTest(config_file, id, num_keys):
    print("Running sanity tests")
    runPutTest(config_file, id, num_keys)
    runGetTest(config_file, id, num_keys)
    print("Sanity tests completed")
