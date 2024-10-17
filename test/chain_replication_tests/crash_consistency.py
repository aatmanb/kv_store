import time
import sys

sys.path.append('../../bin/')
import libkv739_py as kv

max_retries = 5
wait_before_retry = 1
db_keys = {}
overwritten_keys = {}
def populateDB(config_file, data_file, crash_consistency_test=False):
    print ("----------- [test] Start populateDB ------------")
    
    abort = False
    lines = []
    num_keys_populated = 0
    total_duration = 0
    avg_duration = 0

    try:
        # Open a db connection
        kv.init(config_file)
        with open(data_file, "r") as real_keys:
            lines = real_keys.readlines()
            for line in lines:
                start = 0
                stop = 0
                status = -1
                num_retries = 0
                key, value = line.split(',')
                value = value.strip()
                
                # Try inserting key into the database
                while (status == -1):
                    
                    start = time.time_ns()        # Record start time
                    status, _ = kv.put(key, value)
                    end = time.time_ns()          # Record end time

                    if status == -1:
                        print (f"Error: Could\'nt put() key {key} into database")
                        if num_retries == max_retries:
                            if crash_consistency_test:
                                print (f"Error: reached retry limit {max_retries}. Aborting populateDB and moving to get()." )
                                abort = True
                                break
                            else:
                                print (f"Error: reached retry limit {max_retries}. Aborting client.")
                                sys.exit(1)
                        num_retries += 1
                        time.sleep(wait_before_retry)
                
                # If it didn't fail, update the keys
                if not abort:
                    db_keys[key] = value
                    num_keys_populated += 1
                    if status == 0:
                        overwritten_keys[key] = value
                    total_duration += (end - start)

    except FileNotFoundError:
        print ('Error: Could not open file: ' + file)
    
    finally:
        
        # Close connection to DB
        kv.shutdown()
        if num_keys_populated != 0:
            avg_duration = int(total_duration/(1000*num_keys_populated))
        print (f"Populated {num_keys_populated} keys")
        print (f"Average duration for populate {avg_duration} us")
        print ("----------- [test] End populateDB --------------")

    return db_keys, overwritten_keys

