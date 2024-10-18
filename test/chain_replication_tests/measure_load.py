import numpy as np
import pandas as pd
import time
import psutil
import argparse
import signal
import sys

times = np.array([])
cpu_variances = np.array([], dtype=np.float64)
mem_variances = np.array([], dtype=np.float64)
log_dir = ''

def handle_sigterm(signum, frame):
    print("received SIGTERM signal. Writing to csv before exiting")
    global times
    global cpu_variances
    global mem_variances
    global log_dir
    df = pd.DataFrame({'time': times, 'cpu_usage_variance': cpu_variances, 'mem_usage_variance': mem_variances})
    df.to_csv(log_dir + 'cpu_load.csv')
    sys.exit(0)


def track_resource_usage(pids, duration, snapshot_duration, log_dir):
    start = time.time()
    global times
    global cpu_variances
    global mem_variances
    #times = np.array([])
    #cpu_variances = np.array([], dtype=np.float64)
    #mem_variances = np.array([], dtype=np.float64)

    while True:
        if duration:
            if (time.time() - start) > duration:
                break

        cpu_usages = []
        memory_usages = []
        for pid in pids:
            process = psutil.Process(pid)
            cpu_usages.append(process.cpu_percent(interval=None))
            memory_usages.append(process.memory_percent())
        
        cpu_variances = np.append(cpu_variances, np.var(cpu_usages))
        mem_variances = np.append(mem_variances, np.var(memory_usages))
        times = np.append(times, int(time.time()-start))  

        time.sleep(snapshot_duration)
    
    print("Finished observing process load")
    print(cpu_variances)
    print(mem_variances)
    print(times)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--pids', nargs='+', type=int)
    parser.add_argument('--duration', default=None, type=int)
    parser.add_argument('--snapshot-duration', default=2, type=int)
    parser.add_argument('--log-dir', default='out/', type=str)

    args = parser.parse_args()

    signal.signal(signal.SIGTERM, handle_sigterm)
    log_dir = args.log_dir

    track_resource_usage(args.pids, args.duration, args.snapshot_duration, args.log_dir)
