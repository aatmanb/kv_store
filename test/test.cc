#include <iostream>
#include <memory>
#include <string>
#include <cstring>
#include <stdexcept>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <vector>
#include <chrono>
#include <thread>
#include <cassert>
#include <unordered_map>
#include <random>
#include <iomanip>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include "739kv.h"

ABSL_FLAG(std::string, target, "localhost:9876", "Server address");
ABSL_FLAG(int, id, -1, "Name of the server");
ABSL_FLAG(std::string, real, "", "path to real csv file");
ABSL_FLAG(std::string, fake, "", "path to fake csv file"); 
// ABSL_FLAG(bool, crash_consistency_test, false, "True if running crash consistency test");
ABSL_FLAG(int32_t, test_type, 1, "Type of test: 1 (for correctness), 2 (for crash consistency), 3 (for performance)");

int id;

std::unordered_map<std::string, std::string> overwritten_kv;
std::unordered_map<std::string, std::string> db;
std::vector<std::string> keys_populated;
int num_keys_populated = 0;

void runGetTest(std::string target_str, uint16_t name) {
    printf("----------- [test] Start GetTest ------------\n");
    std::string test_key = "connection_test_key";
    std::string test_value;
    int status = -1; // Error by default
    kv739_init(target_str); 
    for (int i=0; i<1000; i++) { 
        status = kv739_get(test_key, test_value);
        std::cout << "[client " << name << "] " << "status: " << status << " " << "get" << "(" << test_key << ")" << ": " << test_value << std::endl;
    }
    kv739_shutdown();
    printf("----------- [test] End GetTest ------------\n");
}

void runPutTest(std::string target_str, uint16_t name) {
    printf("----------- [test] Start PutTest ------------\n");
    std::string test_key = "connection_test_key";
    std::string test_value = "connection_test_value";
    std::string old_value;
    int status = -1; // Error by default
 
    kv739_init(target_str);
    status = kv739_put(test_key, test_value, old_value);
    kv739_shutdown();
    printf("----------- [test] End PutTest ------------\n");
}

int getKeyNumber(float skew_factor) {
    assert(skew_factor >= 0);
    assert(skew_factor <= 1);
    int frequent_group_size = std::max(1, static_cast<int>(skew_factor * num_keys_populated));
    int infrequent_group_size = num_keys_populated - frequent_group_size;

    std::vector<int> frequent_group;
    std::vector<int> infrequent_group;

    for (int i = 0; i < frequent_group_size; ++i) {
        frequent_group.push_back(i);
    }

    for (int i = frequent_group_size; i < num_keys_populated; ++i) {
        infrequent_group.push_back(i);
    }

    // Initialize the random engine and distribution
    std::random_device rd;  // Random seed
    std::mt19937 gen(rd()); // Mersenne Twister random number engine
    std::uniform_real_distribution<> dist(0.0, 1.0);

    // Select the group based on the weighted probability (90% for frequent, 10% for infrequent)
    double random_value = dist(gen);  // Generate a random value between 0 and 1
    if (random_value < (1-skew_factor)) {  // 90% probability
        // Choose a number from the frequent group
        std::uniform_int_distribution<> dist_frequent(0, frequent_group.size() - 1);
        return frequent_group[dist_frequent(gen)];
    } else {
        // Choose a number from the infrequent group
        std::uniform_int_distribution<> dist_infrequent(0, infrequent_group.size() - 1);
        return infrequent_group[dist_infrequent(gen)];
    }
}

int populateDB(std::string target_str, std::string fname, bool crash_consistency_test) {
    printf("----------- [test] Start populateDB ------------\n");
    std::ifstream fp;
    std::string line;
    int status;
    int num_retry;
    bool abort = false;
    int total_duration = 0;
    int avg_duration = 0;
    num_keys_populated = 0;

    fp.open(fname);

    if (!fp.is_open()) {
        std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Error: couldn't open file " << fname << std::endl;
        std::exit(1);
    }

    kv739_init(target_str);
    while(std::getline(fp, line)) {
        std::chrono::high_resolution_clock::time_point start;
        std::chrono::high_resolution_clock::time_point end;

        std::stringstream ss(line);
        std::string part;
        std::vector<std::string> row;
        
        status = -1;
        num_retry = 0;

        while(std::getline(ss, part, ',')) {
            row.push_back(part);
        }

        std::string key = row[0];
        std::string value = row[1];
        std::string old_value;
        while (status == -1) {
            // Record the start time
            start = std::chrono::high_resolution_clock::now();
            status = kv739_put(key, value, old_value);
            // Record the end time
            end = std::chrono::high_resolution_clock::now();

            if (status == -1) {
                std::cout << __FILE__ << "[" << __LINE__ << "]" << "Error: couldn't put() key into database" << std::endl;
                if (num_retry == max_retry) {
                    if (crash_consistency_test) {
                        std::cout << __FILE__ << "[" << __LINE__ << "]" << "Error: reached retry limit " << max_retry << ". Aborting populateDB and moving to get(). client:  " << id << std::endl;
                        abort = true;
                        break;
                    }
                    else {
                        std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Error: reached retry limit " << max_retry << " . Exiting client " << id << std::endl;
                        std::exit(1);
                    }
                }
                num_retry++;
                std::this_thread::sleep_for(std::chrono::seconds(wait_before_retry));
            }
        }
        
        if (!abort) {
            // Value which are written successfully in DB. Used for crash consistency test.
            num_keys_populated++;
            db.insert(std::make_pair(key, value));
            keys_populated.push_back(key);
            
            // Output the duration
            // Calculate the duration in microseconds, milliseconds, or any other unit
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
            total_duration += duration.count();
 
            if (status == 0) {
                // There was an old value. This key is overwriting the old value.
                // Add it to overwritten map
                overwritten_kv.insert(std::make_pair(key, value));
            }
        }
        else {
            // Aborted populateDB
            break;
        }
    }
    kv739_shutdown();

    fp.close();

    avg_duration = total_duration/num_keys_populated;

    std::cout << "Populated " << num_keys_populated << " keys" << std::endl;
    printf("----------- [test] End populateDB ------------\n");
    
    return avg_duration;
}

int runCorrectnessTest(std::string target_str, std::string real, std::string fake, bool crash_consistency_test) {
    printf("----------- [test] Start Correctess Test ------------\n");
    std::ifstream fp;
    std::string line;
    int status = -1;
    int num_retry = 0;
    bool pass = true;
    int num_reads_performed = 0;
    int num_reads_to_perform = num_keys_populated * 10;
    int key_number;
    int total_duration = 0;
    int avg_duration = 0;
    
    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point end;

    fp.open(real);
    
    if (!fp.is_open()) {
        std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Error: couldn't open file " << real << std::endl;
        std::exit(1);
    }

    std::cout << std::endl;
    std::cout << "Testing real keys" << std::endl;
    std::cout << std::endl;

    kv739_init(target_str); 
    //while(std::getline(fp, line)) {
    for (num_reads_performed=0; num_reads_performed < num_reads_to_perform; num_reads_performed++) {
        key_number = getKeyNumber(0.1);
        
        std::stringstream ss(line);
        std::string part;
        std::vector<std::string> row;
        
        status = -1;
        num_retry = 0;

        while(std::getline(ss, part, ',')) {
            row.push_back(part);
        }

        std::string key = keys_populated[key_number];
        std::string correct_value = db.find(key) -> second;
        //std::string key = row[0];
        //std::string correct_value = row[1];
        std::string value;
        while (status == -1) {
            start = std::chrono::high_resolution_clock::now();
            status = kv739_get(key, value);
            end = std::chrono::high_resolution_clock::now();

            if (status == -1) {
                std::cout << __FILE__ << "[" << __LINE__ << "]" << "Error: couldn't get() value from database" << std::endl;
                if (num_retry == max_retry) {
                    std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Error: reached retry limit " << max_retry << " . Exiting client " << id << std::endl;
                    std::exit(1);
                }
                if (!crash_consistency_test) {
                    // Retries are expected for crash consistency test so we don't error out
                    num_retry++;
                }
                std::this_thread::sleep_for(std::chrono::seconds(wait_before_retry));
            }
            else {
                // Output the duration
                // Calculate the duration in microseconds, milliseconds, or any other unit
                auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
                total_duration += duration.count();
 
                if (auto search = overwritten_kv.find(key); search != overwritten_kv.end()) {
                    // If a key is was overwritten, use the latest value
                    std::cout << "encountered overwritten key" << std::endl;
                    correct_value = search->second;
                }
                if (correct_value != value) {
                    pass = false;
                    std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Correctness Test Failed!!" << std::endl;
                    std::cerr << "key: " << key << std::endl;
                    std::cerr << "correct_value: " << correct_value << std::endl;
                    std::cerr << "retrieved value: " << value << std::endl;
                }
            }
        }
    }

    fp.close();

    avg_duration = total_duration / (num_reads_performed-1);

    fp.open(fake);    
    if (!fp.is_open()) {
        std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Error: couldn't open file " << fake << std::endl;
        std::exit(1);
    }

    std::cout << std::endl;
    std::cout << "Testing fake keys" << std::endl;
    std::cout << std::endl;

    while(std::getline(fp, line)) {
        std::stringstream ss(line);
        std::string part;
        std::vector<std::string> row;
        
        status = -1;
        num_retry = 0;

        while(std::getline(ss, part, ',')) {
            row.push_back(part);
        }

        std::string key = row[0];
        std::string value;
        while (status == -1) {
            status = kv739_get(key, value);

            if (status == -1) {
                std::cout << __FILE__ << "[" << __LINE__ << "]" << "Error: couldn't get() value from database" << std::endl;
                if (num_retry == max_retry) {
                    std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Error: reached retry limit " << max_retry << " . Exiting client " << id << std::endl;
                    std::exit(1);
                }
                if (!crash_consistency_test) {
                    // Retries are expected for crash consistency test so we don't error out
                    num_retry++;
                }
                std::this_thread::sleep_for(std::chrono::seconds(wait_before_retry));
            }
            if (status == 0) { // '0' means value was retrived successfully which is not expected
                pass = false;
                std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Correctness Test Failed!!" << std::endl;
                std::cerr << "key: " << key << std::endl;
                std::cerr << "database should not contain this key" << std::endl;
            }
        }

    }
    fp.close();
    
    kv739_shutdown();

    if (pass) {
        std::cout << "Test Passed" << std::endl;
    }
    else {
        std::cout << "Test Failed" << std::endl;
    }

    printf("----------- [test] End Correctness Test ------------\n");

    return avg_duration;
} 

template<typename T>
T calc_percentile(std::vector<T> &arr, int num) {
    int idx = int((num * (arr.size() - 1))/100);
    return arr[idx];
}

std::string generate_random_value(std::uniform_int_distribution<> &dist_length, std::mt19937 &gen,
        std::uniform_int_distribution<> &char_dist) {
    int length = dist_length(gen);
    std::string res;
    for (int i=0; i<length; i++) {
        res.push_back((char)('a' + char_dist(gen)));
    }
    return res;
}

void run_performance_test(std::string target_str, int write_percentage=10) {
    const std::vector<int> percentiles = {50, 70, 90, 99};
    int key_number;
    int total_duration = 0;
    int avg_duration = 0;
    int num_ops = num_keys_populated * 20;
    
    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point end;

    kv739_init(target_str); 

    // Initialize the random engine and distribution
    std::random_device rd;  // Random seed
    std::mt19937 gen(rd()); // Mersenne Twister random number engine
    std::uniform_int_distribution<> dist(0, 99);
    std::uniform_int_distribution<> dist_value_length(10, 2048);
    std::uniform_int_distribution<> char_dist(0, 25);
    std::string key, value, old_value;
    int status;

    int64_t total_write_time = 0ll;
    int64_t total_read_time = 0ll;
    int64_t num_read_failures = 0ll;
    int64_t num_write_failures = 0ll;

    std::vector<int64_t> read_times;
    std::vector<int64_t> write_times;

    auto experiment_start = std::chrono::high_resolution_clock::now();

    for (int i=0; i<num_ops; i++) {
        int random_value = dist(gen);
        key_number = getKeyNumber(0.1);
        key = keys_populated[key_number];
        if (random_value < write_percentage) {
            // perform put
            start = std::chrono::high_resolution_clock::now();
            value = std::move(generate_random_value(dist_value_length, gen, char_dist));
            status = kv739_put(key, value, old_value);
            end = std::chrono::high_resolution_clock::now();
            if (status != -1) {
                total_write_time += std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
                write_times.push_back(std::chrono::duration_cast<std::chrono::microseconds>(end-start).count());
            } else {
                num_write_failures++;
            }
        } else {
            // perform get
            start = std::chrono::high_resolution_clock::now();
            status = kv739_get(key, value);
            end = std::chrono::high_resolution_clock::now();
            if (status != -1) {
                total_read_time += std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
                read_times.push_back(std::chrono::duration_cast<std::chrono::microseconds>(end-start).count());
            } else {
                num_read_failures++;
            }
        }
    }
    auto experiment_end = std::chrono::high_resolution_clock::now();
    auto experiment_duration = std::chrono::duration_cast<std::chrono::milliseconds>(experiment_end - experiment_start).count(); 

    float throughput = (num_ops * /*convert to seconds*/1000.0f/experiment_duration);

    std::cout << "Finished performance tests.....\n";
    std::cout << "No. of read failures: " << num_read_failures << "\n";
    std::cout << "No. of write failures: " << num_write_failures << "\n";
    std::cout << "Percentage of writes: " << write_percentage << "\n";

    sort(read_times.begin(), read_times.end());
    sort(write_times.begin(), write_times.end());

    std::cout << "Read times:\n";
    for (int i: percentiles) {
        std::cout << i << "th percentile: " << calc_percentile<>(read_times, i) << "\n";
    }
    std::cout << "................................\n";

    std::cout << "Write times:\n";
    for (int i: percentiles) {
        std::cout << i << "th percentile: " << calc_percentile<>(write_times, i) << "\n";
    }
    std::cout << "................................\n\n";
    std::cout << std::setprecision(2);
    std::cout << "Throughput: " << throughput << "\n";
}

int main(int argc, char** argv) {
    absl::ParseCommandLine(argc, argv);

    id = absl::GetFlag(FLAGS_id);
    try {
        if (id == -1) {
            throw std::invalid_argument("Server id invalid. Please pass a positive integer as the server id.");
        }
    }
    catch (const std::invalid_argument &e) {
        std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Error: " << e.what() << std::endl;
        std::exit(1);
    }

    // Instantiate the client. It requires a channel, out of which the actual RPCs
    // are created. This channel models a connection to an endpoint specified by
    // the argument "--target=" which is the only expected argument.
    std::string target_str = absl::GetFlag(FLAGS_target);
    std::string real_fname = absl::GetFlag(FLAGS_real);
    std::string fake_fname = absl::GetFlag(FLAGS_fake);
    // bool crash_consistency_test = absl::GetFlag(FLAGS_crash_consistency_test);
    int32_t test_type = absl::GetFlag(FLAGS_test_type);

    runPutTest(target_str, id);
    //runGetTest(target_str, id); 
    
    //bool crash_consistency_test = (test_type == 2);
    //bool performance_test = (test_type == 3);
    //int populate_duration_1 = populateDB(target_str, real_fname, crash_consistency_test);

    //if (performance_test) {
    //    std::cout << "Starting performance test......";
    //    run_performance_test(target_str, 10);
    //} else {
    //    std::cout << "crash_consistency_test: " << crash_consistency_test << std::endl;
    //    int read_duration = runCorrectnessTest(target_str, real_fname, fake_fname, crash_consistency_test);

    //    std::cout << "populate_duration_1: " << populate_duration_1 << " us" << std::endl;
    //    std::cout << "read_duration: " << read_duration << " us" << std::endl;
    //}

    return 0;
}
