import argparse
import sys
import os
import subprocess
import time

import crash_consistency
import correctness
import performance
import sanity


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('--id', type=int, default=1, help='Client id')
    parser.add_argument('--config-file', type=str, default='chain_config.txt', help='chain configuration file')
    parser.add_argument('--real-fname', type=str, default='real')
    parser.add_argument('--fake-fname', type=str, default='fake')
    parser.add_argument('--test-type', type=str, default='sanity', help='sanity, correctness, crash_consistency, perf')
    parser.add_argument('--top-dir', type=str, default='', help='path to top dir')
    parser.add_argument('--log-dir', type=str, default='out/', help='path to log dir')
    parser.add_argument('--num-keys', type=int, default=1, help='number of gets to put and get in sanity test')

    args = parser.parse_args()
    
    if (args.top_dir):
        top_dir = args.top_dir
    else:
        top_dir = '../../'

    client_id = args.id
    if (id == -1): 
        print ('Client id invalid. Please pass a positive integer as the client id.') 
        sys.exit(1)

    test_type = args.test_type
    config_file = top_dir + args.config_file
    
    bin_dir = top_dir + 'bin/'
    db_dir = top_dir + 'db/'
    log_dir = top_dir + args.log_dir
    
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    crash_consistency_test = (test_type == 'crash_consistency')
    performance_test = (test_type == 'perf')

    db_keys = {}
    overwritten_keys = {}

    try:
        if (test_type == 'sanity'):
            sanity.runSanityTest(config_file, id, args.num_keys)
        elif (test_type == 'perf'):
            # Populate DB
            db_keys, overwritten_keys = crash_consistency.populateDB(config_file, args.real_fname, crash_consistency_test)
            performance.performanceTest(config_file, 10, db_keys)
        elif (test_type == 'crash_consistency'):
            correctness.correctnessTest(config_file, args.fake_fname, db_keys, overwritten_keys, crash_consistency_test)
        elif (type_type == 'correctness'):
            correctness.correctnessTest(config_file, args.fake_fname, db_keys, overwritten_keys)
        else:
            raise ValueError(f"Invalid test type {test_type}")
    except Exception as e:
        print(f"An unexpected exception occured: {e}")
        sys.exit(1)
