#include <iostream>
#include <memory>
#include <string>
#include <cstring>
#include <stdexcept>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include <grpcpp/grpcpp.h>

#include "739kv.h"

ABSL_FLAG(std::string, target, "localhost:50051", "Server address");
ABSL_FLAG(int, name, -1, "Name of the server");
ABSL_FLAG(std::string, real, "", "path to real csv file");
ABSL_FLAG(std::string, fake, "", "path to fake csv file"); 

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

void populateDB(std::string target_str, std::string fname) {
    printf("----------- [test] Start populateDB ------------\n");
    std::ifstream file(fname);
    std::string line;
    int status;

    if (!file.is_open()) {
        std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Error: couldn't open file " << fname << std::endl;
        return;
    }

    kv739_init(target_str); 
    while(std::getline(file, line)) {
        std::stringstream ss(line);
        std::string part;
        std::vector<std::string> row;
        
        while(std::getline(ss, part, ',')) {
            row.push_back(part);
        }

        std::string key = row[0];
        std::string value = row[1];
        std::string old_value;
        status = kv739_put(key, value, old_value);

        if (status == -1) {
            std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Error: couldn't put() key into database" << std::endl;
            std::exit(1);
        }
    }
    kv739_shutdown();

    printf("----------- [test] End populateDB ------------\n");
}

void runCorrectnessTest(std::string target_str, std::string real, std::string fake) {
    printf("----------- [test] Start Correctess Test ------------\n");
    std::ifstream real_fp(real);
    std::ifstream fake_fp(fake);
    std::string line;
    int status;

    if (!real_fp.is_open()) {
        std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Error: couldn't open file " << real << std::endl;
        return;
    }

    if (!fake_fp.is_open()) {
        std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Error: couldn't open file " << fake << std::endl;
        return;
    }

    kv739_init(target_str); 
    while(std::getline(real_fp, line)) {
        std::stringstream ss(line);
        std::string part;
        std::vector<std::string> row;
        
        while(std::getline(ss, part, ',')) {
            row.push_back(part);
        }

        std::string key = row[0];
        std::string correct_value = row[1];
        std::string value;
        status = kv739_get(key, value);

        if (status == -1) {
            std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Error: couldn't get() value from database" << std::endl;
            std::exit(1);
        }
        
        if (correct_value != value) {
            std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Correctness Test Failed!!" << std::endl;
            std::cerr << "key: " << key << std::endl;
            std::cerr << "correct_value: " << correct_value << std::endl;
            std::cerr << "retrieved value: " << value << std::endl;
        }
    }
    
    while(std::getline(fake_fp, line)) {
        std::stringstream ss(line);
        std::string part;
        std::vector<std::string> row;
        
        while(std::getline(ss, part, ',')) {
            row.push_back(part);
        }

        std::string key = row[0];
        std::string value;
        status = kv739_get(key, value);

        if (status == -1) {
            std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Error: couldn't get() value from database" << std::endl;
            std::exit(1);
        }
        
        if (status == 0) { // '0' means value was retrived successfully which is not expected
            std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Correctness Test Failed!!" << std::endl;
            std::cerr << "key: " << key << std::endl;
            std::cerr << "database should contain this key" << std::endl;
        }
    }
    
    kv739_shutdown();

    printf("----------- [test] End Correctness Test ------------\n");
} 

int main(int argc, char** argv) {
    absl::ParseCommandLine(argc, argv);

    int name = absl::GetFlag(FLAGS_name);
    try {
        if (name == -1) {
            throw std::invalid_argument("Server name invalid. Please pass a positive integer as the server name.");
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
  
    populateDB(target_str, real_fname);

    runCorrectnessTest(target_str, real_fname, fake_fname);
    //runPutTest(target_str, name);
    //runGetTest(target_str, name); 

    return 0;
}
