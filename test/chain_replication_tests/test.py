import argparse
import sys
import os
import subprocess
import time

import crash_consistency
import correctness
import performance
import sanity

bin_dir = ''
db_dir = ''
log_dir = ''

server_processes = []
client_processes = []

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
    #TODO: start the manager before creating chains

    partitions = getPartitionConfig(config_file)
    for _, servers in partitions.items():
        createChain(servers)
        
def terminateService():
    for process in server_processes:
        process.terminate()
        returncode = process.wait()
        print(f"termination return code: {returncode}")

def startClients(args):
    for client_id in range(args.num_clients):
        cmd = 'python3 client.py'
        cmd += ' ' + f'--id={client_id}'
        cmd += ' ' + f'--config-file={args.config_file}'
        cmd += ' ' + f'--real-fname={args.real_fname}'
        cmd += ' ' + f'--fake-fname={args.fake_fname}'
        cmd += ' ' + f'--test-type={args.test_type}'
        cmd += ' ' + f'--top-dir={args.top_dir}'
        cmd += ' ' + f'--log-dir={args.log_dir}'
        
        print(f"Starting client {client_id}")
        print(cmd)
        log_file = log_dir + f'client_{client_id}.log'

        with open(log_file, 'w') as f:
            process = subprocess.Popen(cmd, shell=True, stdout=f, stderr=f)
            client_processes.append(process)


def terminateClients():
    for process in client_processes:
        process.terminate()
        returncode = process.wait()
        print(f"termination return code: {returncode}")

def terminateTest():
    terminateClients()
    terminateService()
    sys.exit(1)   

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('--config-file', type=str, default='chain_config.txt', help='chain configuration file')
    parser.add_argument('--real-fname', type=str, default='real')
    parser.add_argument('--fake-fname', type=str, default='fake')
    parser.add_argument('--test-type', type=str, default='sanity', help='sanity, correctness, crash_consistency, perf')
    parser.add_argument('--top-dir', type=str, default='../../', help='path to top dir')
    parser.add_argument('--log-dir', type=str, default='out/', help='path to log dir')
    parser.add_argument('--num-clients', type=int, default=1, help='number of clients')

    args = parser.parse_args()
    
    top_dir = args.top_dir
    test_type = args.test_type
    config_file = top_dir + args.config_file
    
    bin_dir = top_dir + 'bin/'
    db_dir = top_dir + 'db/'
    log_dir = top_dir + args.log_dir
    
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    #try:
    #    createService(config_file)
    #except Exception as e:
    #    print(f"An unexpected exception occured: {e}")
    #    terminateTest()
    #    
    #time.sleep(5)

    try:
        startClients(args)
    except Exception as e:
        print(f"An unexpected exception occured: {e}")
        terminateTest()

    print("Test finished. Terminating service")
    terminateService()
    
    print("Test End")
