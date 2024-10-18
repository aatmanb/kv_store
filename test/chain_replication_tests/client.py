import argparse
import sys
import os
import subprocess
import time

import crash_consistency
import correctness
import performance_test
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
    parser.add_argument('--num-keys', type=int, default=5, help='number of gets to put and get in sanity test')
    parser.add_argument('--vk_ratio', type=int, default=0, help='ratio of value length to key length')
    parser.add_argument('--skew', action='store_true')
 
    args = parser.parse_args()
    
    if (args.top_dir):
        top_dir = args.top_dir
    else:
        top_dir = '../../'

    client_id = args.id
    vk_ratio = args.vk_ratio
    if (id == -1): 
        print ('Client id invalid. Please pass a positive integer as the client id.') 
        sys.exit(1)

    test_type = args.test_type
    config_file = top_dir + args.config_file
    
    bin_dir = top_dir + 'bin/'
    db_dir = top_dir + 'db/'
    log_dir = top_dir + args.log_dir
    skew = args.skew
    
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    db_keys = {}
    overwritten_keys = {}
    keys = []
    values = []
    try:
        if (test_type == 'sanity'):
            sanity.runSanityTest(config_file, client_id, args.num_keys)

        elif (test_type == 'perf'):
            num_keys = 600
            # Populate DB
            keys, values = performance_test.populateDB(config_file, client_id, num_keys, vk_ratio)
            if (vk_ratio != 0):
                performance_test.performanceTest(config_file, vk_ratio, num_keys, 0, skew, keys, values)
            else:
                performance_test.performanceTest(config_file, vk_ratio, num_keys, 10, skew, keys, values)

        elif (test_type == 'crash_consistency'):
            correctness.correctnessTest(config_file, args.fake_fname, db_keys, overwritten_keys, crash_consistency_test)

        elif (test_type == 'correctness'):
            # Populate DB
            db_keys, overwritten_keys = crash_consistency.populateDB(config_file, args.real_fname, crash_consistency_test)
            correctness.correctnessTest(config_file, args.fake_fname, db_keys, overwritten_keys)
        else:
            raise ValueError(f"Invalid test type {test_type}")
    except Exception as e:
        print(f"An unexpected exception occured: {e}, {args}")
        sys.exit(1)
