# kv_store

TODOs:
1. Testing
    a. Split test.cc into multiple files - one for each type of test: p0 test.cc -> test.py
    b. Move test.cc and split files from src/ to a common directory with test kv pair generator: p5
    c. Dump test values like latency, throughput in a csv file instead of printing on stdout: p0
    d. Add sanity test to ensure good enough connection by measuring server ping: p0
2. Debug
    a. Add a debug flag which can be turned off from command line when needed. Maybe like gem5?: p2
3. Separate directories for server, client and utils under src/ : p0
4. All group members should use a common directory structure i.e. common direcotory where binaries are built
5. Expose the following command line options: p0
    a. Server
        i. Number of partitions
        ii. Cache size
        iii. Communication port(s)
        iv. DB directory
    b. Clients
        i. Communication port(s)
        ii. Client id
        iii. test data file path
        iv. client connection timeout
6. Dockerfile: p1
7. rocksDB instead of sqlite3: Ask mike if we can use it
