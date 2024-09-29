#include <iostream>
#include <memory>
#include <string>
#include <cstring>
#include <stdexcept>
#include <cstdlib>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include <grpcpp/grpcpp.h>

#include "739kv.h"

ABSL_FLAG(std::string, target, "localhost:50051", "Server address");
ABSL_FLAG(int, name, -1, "Name of the server"); 

void runGetTest(std::string target_str, uint16_t name) {
    printf("----------- [test] Start GetTest ------------\n");
    char *test_key = "connection_test_key";
    char *test_value = new char[value_max_len + 1];
    int status = -1; // Error by default
    kv739_init(stringToCharArray(target_str)); 
    for (int i=0; i<10; i++) { 
        status = kv739_get(test_key, test_value);
    }
    std::cout << "[client " << name << "] " << "status: " << status << " " << "get" << "(" << test_key << ")" << ": " << test_value << std::endl;
    kv739_shutdown();
    printf("----------- [test] End GetTest ------------\n");
}

void runPutTest(std::string target_str, uint16_t name) {
    printf("----------- [test] Start PutTest ------------\n");
    char *test_key = "connection_test_key";
    char *test_value = "connection_test_value";
    char *old_value = new char[value_max_len + 1];
    int status = -1; // Error by default
 
    status = kv739_put(test_key, test_value, old_value);
    printf("----------- [test] End PutTest ------------\n");
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
        std::cerr << "Error: " << e.what() << std::endl;
        std::exit(1);
    }

    // Instantiate the client. It requires a channel, out of which the actual RPCs
    // are created. This channel models a connection to an endpoint specified by
    // the argument "--target=" which is the only expected argument.
    std::string target_str = absl::GetFlag(FLAGS_target);
   
    runGetTest(target_str, name); 
    runPutTest(target_str, name);

    return 0;
}
