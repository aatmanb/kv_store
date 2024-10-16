import argparse
import sys
import crash_consistency
import correctness
import performance
import sanity

bin_dir = ''

def getPartitionConfig(config_file):
    # initialize the partition dictionary
    partitions = {}
    with open(config_filei, "r") as f:
        num_partitions  = int(f.readline())
        
        for i in range(num_partitions):
            partitions[i] = []

        for line in f:
            server = line.split(',')
            assert(len(server) == 3)
            parition_id = int(server[0])
            server_id = server[1]
            port = server[2]
            partitions[partition_id].append(port)
    
    # shouldn't be empty
    assert(partitions) 
    
    return partitions


def startServer(config, head=false, tail=false):
    cmd = bin_dir + ' 

def createChain(server_list):
    if (len(server_list) == 1):
        startServer(server_list[0], head=true, tail=true)
        return

    head = server_list[0]
    startServer(head, head=true, tail=false)
    for server in servers[1:-1]:
        startServer(server, head=false, tail=false)        
    tail = server_list[-1]
    startServer(head, head=false, tail=true)

    return

def createService(config_file, bin_dir):
    partitions = getPartitionConfig(config_file)
    for _, servers in partitions.items():
        createChain(servers)
        
        

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('--id', type=int, default=1, help='Client id')
    parser.add_argument('--config-file', type=str, default='chain_config.txt', help='chain configuration file')
    parser.add_argument('--real-fname', type=str, default='real')
    parser.add_argument('--fake-fname', type=str, default='fake')
    parser.add_argument('--test-type', type=str, default='sanity', help='sanity, correctness, crash_consistency, perf')
    parser.add_argument('--top-dir'. type=str, default='', help='path to top dir')

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
    global bin_dir = top_dir + 'bin/'

    crash_consistency_test = (test_type == 'crash_consistency')
    performance_test = (test_type == 'perf')

    db_keys = {}
    overwritten_keys = {}

    # Populate DB
    db_keys, overwritten_keys = crash_consistency.populateDB(config_file, args.real_fname, crash_consistency_test)

    if (test_type == 'sanity'):
        sanity.runSanityTest(config_file, id)
    elif (test_type == 'perf'):
        performance.performanceTest(config_file, 10, db_keys)
    elif (test_type == 'crash_consistency'):
        correctness.correctnessTest(config_file, args.fake_fname, db_keys, overwritten_keys, crash_consistency_test)
    elif (type_type == 'correctness'):
        correctness.correctnessTest(config_file, args.fake_fname, db_keys, overwritten_keys)
    else:
        raise ValueError(f"Invalid test type {test_type}")
