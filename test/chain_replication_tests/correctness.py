import numpy as np
import time
import sys

sys.path.append('../../bin/')
import libkv739_py as kv

max_retries = 5
wait_before_retry = 1

def getKeyNumber(skewFactor, num_keys_populated):
    assert (skewFactor >= 0) and (skewFactor <= 1), "Invalid skew factor"
    freq_gp_size = max(1, int(skewFactor * num_keys_populated))

    random_value = np.random.uniform(0,1)
    if (random_value < (1-skewFactor)):
        return np.random.randint(freq_gp_size)
    return np.random.randint(freq_gp_size, num_keys_populated)

def correctnessTest(config_file, fake_keys, db_keys, overwritten_keys, crash_consistency_test = False):
    
    if (len(db_keys) == 0):
        print ('db_keys is not populated. Populated the db first')
        return None
    
    print ("----------- [test] Start Correctness Test ------------") 
    num_keys_populated = len(db_keys) 
    num_reads = 10 * num_keys_populated 
    keys_populated = list(db_keys.keys())
    test_passed = True
    total_duration = 0
    avg_duration = 0
        
    print ('Testing Real Keys')
    
    kv.init(config_file)
    for num_reads_performed in range (0, num_reads):
        status = -1
        num_retries = 0
        key_number = getKeyNumber(0.1, num_keys_populated)
        key = keys_populated[key_number]
        correct_value = db_keys[key]
        value = ''
                
        # Get keys from db
        while (status == -1):
            start = time.time_ns()
            status, value = kv.get(key)
            end = time.time_ns()
            
            if status == -1:
                print (f"Error: Couldn\'t get() key {key} from database")
                if num_retries == max_retries:
                    print (f"Error: reached retry limit {max_retries}. Aborting client.")
                    sys.exit(1)                    
                if not crash_consistency_test:
                    num_retries += 1
                time.sleep(wait_before_retry)
        
            else:
                total_duration += (end - start)
                if key in overwritten_keys:
                    print ('Encountered overwritten key')
                    correct_value = overwritten_keys[key]
                if value != correct_value:
                    test_passed = False
                    print ('Correctness Test Failed!!')
                    print (f"key: {key}")
                    print (f"correct_value: {correct_value}")
                    print (f"retrieved value: {value}") 

    try:
        lines = []
        with open(fake_keys, "r") as fake:
            print ('Testing Fake Keys')
            lines = fake.readlines()
            for line in lines:
                key, value = line.split(',')
                
                # Check if Get succeeds here
                status, obtained_value = kv.get(key)

                if status == -1:
                    print (f"Error: Couldn\'t get() key {key} from database")
                    if num_retries == max_retries:
                        print (f"Error: reached retry limit {max_retries}. Aborting client.")
                        sys.exit(1)                    
                    if not crash_consistency_test:
                        num_retries += 1
                    time.sleep(wait_before_retry)

                elif status == 0:     # We should not be getting these values in the database
                    test_passed = False
                    print ('Correctness Test Failed!!')
                    print (f"key: {key}")
                    print ('Database shouldn\'t contain this value')
       
    except FileNotFoundError:
        print ('Error: Could not open file: ' + file)

    finally:
        
        kv.shutdown()
        avg_duration = int(total_duration/(1000*num_reads))
        if (test_passed):
            print ('Test Passed!')
        else:
            print ('Test Failed!')

        print ("----------- [test] End Correctness Test ------------") 
                
