import argparse
import sys
import crash_consistency
import correctness
import performance

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('--id', type=int, default=1, help='Client id')
    parser.add_argument('--config-file', type=str, default="", help='chain configuration file')
    parser.add_argument('--real-fname', type=str, default='real')
    parser.add_argument('--fake-fname', type=str, default='fake')
    parser.add_argument('--test_type', type=int, default=1, help='Type of test: 1 (for correctness), 2 (for crash consistency), 3 (for performance)')

    args = parser.parse_args()
    
    client_id = args.id
    if (id == -1): 
        print ('Client id invalid. Please pass a positive integer as the client id.') 
        sys.exit(1)

    test_type = args.test_type
    config_file = args.config_file

    crash_consistency_test = (test_type == 2)
    performance_test = (test_type == 3)

    db_keys = {}
    overwritten_keys = {}

    # Populate DB
    db_keys, overwritten_keys = crash_consistency.populateDB(config_file, args.real_fname, crash_consistency_test)

    if performance_test:
        performance.performanceTest(config_file, 10, db_keys)
    elif crash_consistency_test:
        correctness.correctnessTest(config_file, args.fake_fname, db_keys, overwritten_keys, crash_consistency_test)
    else:
        correctness.correctnessTest(config_file, args.fake_fname, db_keys, overwritten_keys) 
