import argparse
import sys
import os
import crash_consistency
import correctness
import performance
import sanity
import subprocess

bin_dir = ''
db_dir = ''
log_dir = ''

server_processes = []

def getPartitionConfig(config_file):
    # initialize the partition dictionary
    partitions = {}
    with open(config_file, "r") as f:
        num_partitions  = int(f.readline())
        
        for i in range(num_partitions):
            partitions[i] = []

        for line in f:
            server = line.split(',')
            assert(len(server) == 3)
            partition_id = int(server[0].strip())
            server_id = server[1].strip()
            port = server[2].strip()
            partitions[partition_id].append((server_id,port))
    
    # shouldn't be empty
    assert(partitions) 
    
    return partitions


def startServer(config, head=False, tail=False, head_port='', tail_port='', prev_port='', next_port=''):
    cmd = bin_dir + 'server'
    server_id = config[0]
    cmd += ' ' + f'--id={server_id}'
    cmd += ' ' + f'--db_dir={db_dir}'
    cmd += ' ' + f'--port={config[1]}'

    if head:
        cmd += ' ' + f'--head=true'
    if tail:
        cmd += ' ' + f'--tail=true'

    if head_port:
        cmd += ' ' + f'--head_port={head_port[1]}'

    if tail_port:
        cmd += ' ' + f'--tail_port={tail_port[1]}'

    if prev_port:
        cmd += ' ' + f'--prev_port={prev_port[1]}'

    if next_port:
        cmd += ' ' + f'--next_port={next_port[1]}'

    print(f"Starting server {server_id}")
    print(cmd)
    log_file = log_dir + f'server_{server_id}.log'

    with open(log_file, 'w') as f:
        process = subprocess.Popen(cmd, shell=True, stdout=f, stderr=f)
        server_processes.append(process)


def createChain(server_list):
    if (len(server_list) == 1):
        startServer(server_list[0], head=True, tail=True)
        return

    head = server_list[0]
    for i in range(len(server_list)):
        if (i == 0):
            startServer(server_list[i], head=True, tail_port=server_list[-1], next_port=server_list[i+1])
        elif (i == len(server_list)-1):
            startServer(server_list[i], tail=True, head_port=server_list[0], prev_port=server_list[i-1])
        else:
            startServer(server_list[i], head_port=server_list[0], tail_port=server_list[-1], prev_port=server_list[i-1], next_port=server_list[i+1])        

    return

def createService(config_file):
    partitions = getPartitionConfig(config_file)
    for _, servers in partitions.items():
        createChain(servers)
        
#def createClient(config_file): 

def terminateService():
    for process in server_processes:
        process.terminate()
        process.wait()

def terminateTest():
    terminateService()
    sys.exit(1)   

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('--id', type=int, default=1, help='Client id')
    parser.add_argument('--config-file', type=str, default='chain_config.txt', help='chain configuration file')
    parser.add_argument('--real-fname', type=str, default='real')
    parser.add_argument('--fake-fname', type=str, default='fake')
    parser.add_argument('--test-type', type=str, default='sanity', help='sanity, correctness, crash_consistency, perf')
    parser.add_argument('--top-dir', type=str, default='', help='path to top dir')
    parser.add_argument('--log-dir', type=str, default='out/', help='path to log dir')

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

    try:
        createService(config_file)
    except Exception as e:
        print(f"An unexpected exception occured: {e}")
        terminateTest()
        

    crash_consistency_test = (test_type == 'crash_consistency')
    performance_test = (test_type == 'perf')

    db_keys = {}
    overwritten_keys = {}

    try:
        if (test_type == 'sanity'):
            sanity.runSanityTest(config_file, id)
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
        terminateTest()
        
    print("Test finished. Terminating service")
    terminateService()
    
    print("Test End")
