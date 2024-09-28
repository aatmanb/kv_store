#include <iostream>
#include <memory>
#include <string>
#include <cstring>
#include <stdexcept>
#include <cstdlib>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include <grpcpp/grpcpp.h>
#include "kv_store.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

ABSL_FLAG(std::string, target, "localhost:50051", "Server address");
ABSL_FLAG(uint16_t, name, -1, "Name of the server"); 

const uint16_t key_max_len = 128;
const uint16_t value_max_len = 2048;

// Function to convert std::string to modifiable char*
char* stringToCharArray(const std::string& str) {
    // Allocate memory for the char array (+1 for the null terminator)
    char* charArray = new char[str.length() + 1];
    
    // Copy the contents of the string into the char array
    std::strcpy(charArray, str.c_str());
    
    // Return the modifiable char array
    return charArray;
}

// Function to convert a modifiable char array to std::string
std::string charArrayToString(char* charArray) {
    // Create a std::string from the char array
    return std::string(charArray);
}

class client {
    public:
        client(std::shared_ptr<Channel> channel) : stub_(kv_store::NewStub(channel)) {}

        int kv739_init(char *server_name) {
            //TODO
            return 0;
        }

        int kv739_shutdown() {
            //TODO
            return 0;
        }

        int kv739_get(uint16_t name, char *key, char *value) {
            //TODO
            std::string key_str = charArrayToString(key);
            std::cout << "[client " << name << "] " << "get() called with key: " << key_str << std::endl;
            getReq request;
            request.set_key(key_str);
            request.set_name(name);

            getResp response;

            ClientContext context;

            Status status = stub_->get(&context, request, &response);
            std::strcpy(value, stringToCharArray(response.value()));
            std::cout << "[client " << name << "] " << "response for get" << "(" << key << ")" << ": " << value << std::endl;
            return response.status();
        }

        int kv739_put(uint16_t name, char *key, char *value, char *old_value) {
            std::string key_str = charArrayToString(key);
            std::string value_str = charArrayToString(value);
            
            std::cout << "[client " << name << "] " << "put" << "(" << key_str << ")" << ": " << value_str << std::endl;
            
            putReq request;
            request.set_key(key_str);
            request.set_value(value_str);
            request.set_name(name);

            putResp response;

            ClientContext context;

            Status status = stub_->put(&context, request, &response);
            
            std::strcpy(old_value, stringToCharArray(response.old_value()));
            std::cout << "[client " << name << "] " << "response for put" << "(" << key << ")" << ": " << old_value << std::endl;
            return response.status();
        }

    private:
        std::unique_ptr<kv_store::Stub> stub_;
};

void runGetTest(std::string target_str, uint16_t name) {
    //std::cout << "----------- Start GetTest -------------" << std::endl;
    printf("----------- [client %d] Start GetTest ------------\n", name);
    char *test_key = "connection_test_key";
    char *test_value = new char[value_max_len + 1];
    int status = -1; // Error by default
 
    // We indicate that the channel isn't authenticated (use of
    // InsecureChannelCredentials()).
    client _client(
        grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));
   
    for (int i=0; i<10000; i++) { 
        status = _client.kv739_get(name, test_key, test_value);
    }
    std::cout << "[client " << name << "] " << "status: " << status << " " << "get" << "(" << test_key << ")" << ": " << test_value << std::endl;
    printf("----------- [client %d] End GetTest ------------\n", name);
    //std::cout << "----------- End GetTest -------------" << std::endl;
}

void runPutTest(std::string target_str, uint16_t name) {
    //std::cout << "----------- Start PutTest -------------" << std::endl;
    printf("----------- [client %d] Start PutTest ------------\n", name);
    char *test_key = "connection_test_key";
    char *test_value = "connection_test_value";
    char *old_value = new char[value_max_len + 1];
    int status = -1; // Error by default
 
    // We indicate that the channel isn't authenticated (use of
    // InsecureChannelCredentials()).
    client _client(
        grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));
    
    std::cout << "[client " << name << "] " << "put" << "(" << test_key << ")" << ": " << test_value << std::endl;
    status = _client.kv739_put(name, test_key, test_value, old_value);
    std::cout << "[client " << name << "] " << "status: " << status << " " << "put" << "(" << test_key << ")" << ": " << old_value << std::endl;
    printf("----------- [client %d] End PutTest ------------\n", name);
    //std::cout << "----------- End PutTest -------------" << std::endl;
}


int main(int argc, char** argv) {
    absl::ParseCommandLine(argc, argv);

    uint16_t name = absl::GetFlag(FLAGS_name);
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
