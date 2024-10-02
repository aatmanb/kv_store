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

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include "739kv.h"

ABSL_FLAG(std::string, target, "localhost:50505", "Server address");
ABSL_FLAG(int, id, -1, "Name of the server");
ABSL_FLAG(std::string, real, "", "path to real csv file");
ABSL_FLAG(std::string, fake, "", "path to fake csv file"); 
ABSL_FLAG(bool, crash_consistency_test, false, "True if running crash consistency test"); 

int id;

std::unordered_map<std::string, std::string> overwritten_kv;
int num_keys_populated = 0;

void runGetTest(std::string target_str, uint16_t name) {
    printf("----------- [test] Start GetTest ------------\n");
    std::string test_key = "connection_test_key";
    std::string test_value;
    int status = -1; // Error by default
    kv739_init(target_str); 
    for (int i=0; i<10; i++) { 
        status = kv739_get(test_key, test_value);
    }
    std::cout << "[client " << name << "] " << "status: " << status << " " << "get" << "(" << test_key << ")" << ": " << test_value << std::endl;
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

void populateDB(std::string target_str, std::string fname, bool crash_consistency_test) {
    printf("----------- [test] Start populateDB ------------\n");
    std::ifstream fp;
    std::string line;
    int status;
    int num_retry;
    bool abort = false;
    num_keys_populated = 0;

    fp.open(fname);

    if (!fp.is_open()) {
        std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Error: couldn't open file " << fname << std::endl;
        return;
    }

    kv739_init(target_str); 
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
        std::string value = row[1];
        std::string old_value;
        while (status == -1) {
            status = kv739_put(key, value, old_value);

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

    printf("----------- [test] End populateDB ------------\n");
}

void runCorrectnessTest(std::string target_str, std::string real, std::string fake, bool crash_consistency_test) {
    printf("----------- [test] Start Correctess Test ------------\n");
    std::ifstream fp;
    std::string line;
    int status = -1;
    int num_retry = 0;
    bool pass = true;
    int num_keys_read = 0;

    fp.open(real);
    
    if (!fp.is_open()) {
        std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Error: couldn't open file " << real << std::endl;
        return;
    }

    std::cout << std::endl;
    std::cout << "Testing real keys" << std::endl;
    std::cout << std::endl;

    kv739_init(target_str); 
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
        std::string correct_value = row[1];
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
            else {
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
        num_keys_read++;
        if (num_keys_read == num_keys_populated) break;
    }

    fp.close();

    fp.open(fake);    
    if (!fp.is_open()) {
        std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Error: couldn't open file " << fake << std::endl;
        return;
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
    bool crash_consistency_test = absl::GetFlag(FLAGS_crash_consistency_test);

    std::cout << "crash_consistency_test: " << crash_consistency_test << std::endl;
 
    populateDB(target_str, real_fname, crash_consistency_test);

    runCorrectnessTest(target_str, real_fname, fake_fname, crash_consistency_test);
    //runPutTest(target_str, name);
    //runGetTest(target_str, name); 

    return 0;
}
