import numpy as np
import pandas as pd
import time
import psutil

def track_resource_usage(pids, filename=None):
    DURATION = 20
    SNAPSHOT_DURATION = 2
    start = time.time()
    times = np.array([])
    cpu_variances = np.array([], dtype=np.float64)
    mem_variances = np.array([], dtype=np.float64)

    while True:
        if (time.time() - start) > DURATION:
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

        time.sleep(SNAPSHOT_DURATION)
    
    print("Finished observing process load")
    print(cpu_variances)
    print(mem_variances)
    print(times)

    df = pd.DataFrame({'time': times, 'cpu_usage_variance': cpu_variances, 'mem_usage_variance': mem_variances})
    df.to_csv(filename)

# if __name__ == '__main__':
#     track_resource_usage([1025647, 711746, 711746])
    

