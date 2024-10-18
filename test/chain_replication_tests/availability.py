import time, sys
sys.path.append('../../bin/')

from correctness import getKeyNumber
import libkv739_py as kv

import numpy as np
import pandas as pd
import os
import threading

num_queries = 0
num_failures = 0

def calculate_throughput(client_id):
    global num_queries
    SNAPSHOT_INTERVAL = 2

    print("Launching thread to calculate throughput")
    start = time.time()
    throughput_arr = np.array([], dtype=np.float64)
    fail_ratio = np.array([], dtype=np.float64)
    times = np.array([])
    # Run for 30 mins
    while (time.time() - start <= 1800):
        last_num_queries = num_queries
        last_num_failures = num_failures
        interval_start = time.time()
        time.sleep(SNAPSHOT_INTERVAL)
        throughput = (num_queries - last_num_queries)/(time.time() - interval_start)
        failure_ratio = (num_failures - last_num_failures)/(num_queries-last_num_queries)

        fail_ratio = np.append(fail_ratio, failure_ratio)
        throughput_arr = np.append(throughput_arr, throughput)
        times = np.append(times, int(time.time()-start))

    
    filename = f"{os.getcwd()}/client_{client_id}_throughput.csv"
    df = pd.DataFrame({'time': times, 'throughput': throughput_arr, 'failure_ratio': fail_ratio})
    df.to_csv(filename)


def run_get_queries(config_file, db_keys, client_id):
    global num_queries, num_failures

    thread = threading.Thread(None, calculate_throughput, args=(client_id,))

    max_retries = 1
    if (len(db_keys) == 0):
        print ('db_keys is not populated. Populated the db first')
        return None
    
    print ("----------- [test] Start Availability Test for client ------------") 
    num_keys_populated = len(db_keys)
    num_reads = 10 * num_keys_populated
    keys_populated = list(db_keys.keys())
    total_duration = 0
            
    kv.init(config_file)
    for num_reads_performed in range (num_reads):
        status = -1
        num_retries = 0
        key_number = getKeyNumber(0.1, num_keys_populated)
        key = keys_populated[key_number]
        num_failures = 0
                
        # Get keys from db
        while (status == -1):
            start = time.time_ns()
            status, value = kv.get(key)
            end = time.time_ns()
            
            if status == -1:
                # print (f"Error: Couldn\'t get() key {key} from database")
                if num_retries == max_retries:
                    num_failures+=1
                    break
                    # print (f"Error: reached retry limit {max_retries}. Aborting client.")
                    # sys.exit(1)                    
                num_retries += 1
                # if not crash_consistency_test:
                #     
                # time.sleep(wait_before_retry)
        
            else:
                total_duration += (end - start)
                
        num_queries += 1

    thread.join()
    print ("----------- [test] End Availability Test ------------") 
