#include <iostream>
#include <memory>
#include <string>
#include <cstring>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include <grpcpp/grpcpp.h>
#include "kv_store.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

ABSL_FLAG(std::string, target, "localhost:50051", "Server address");

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

        int kv739_get(const char *key, char *value) {
            //TODO
            std::string key_str(key);
            std::cout << "client: get() called with key: " << key_str << std::endl;
            getReq request;
            request.set_key(key_str);

            getResp response;

            ClientContext context;

            Status status = stub_->get(&context, request, &response);
            std::strcpy(value, stringToCharArray(response.value()));
            std::cout << "client: response for get" << "(" << key << ")" << ": " << value << std::endl;
            return response.status();
        }

        int kv739_put(const char *key, const char *value, char *old_value) {
            std::string key_str(key);
            std::string value_str(value);
            
            std::cout << "client: put" << "(" << key_str << ")" << ": " << value_str << std::endl;
            
            putReq request;
            request.set_key(key_str);
            request.set_value(value_str);

            putResp response;

            ClientContext context;

            Status status = stub_->put(&context, request, &response);
            
            std::strcpy(old_value, stringToCharArray(response.old_value()));
            std::cout << "client: response for put" << "(" << key << ")" << ": " << old_value << std::endl;
            return response.status();
        }

    private:
        std::unique_ptr<kv_store::Stub> stub_;
};

void runGetTest(std::string target_str) {
    std::cout << "----------- Start GetTest -------------" << std::endl;
    std::string test_key = "connection_test_key" + std::to_string(i);
    char *test_value = new char[value_max_len + 1];
    int status = -1; // Error by default
 
    // We indicate that the channel isn't authenticated (use of
    // InsecureChannelCredentials()).
    client _client(
        grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));
    
    status = _client.kv739_get(test_key.c_str(), test_value);
    std::cout << "status: " << status << " " << "get" << "(" << test_key << ")" << ": " << test_value << std::endl;
    std::cout << "----------- End GetTest -------------" << std::endl;
}

void runPutTest(std::string target_str) {
    std::cout << "----------- Start PutTest -------------" << std::endl;
    std::string test_key = "connection_test_key";
    std::string test_value = "connection_test_value";
    char *old_value = new char[value_max_len + 1];
    int status = -1; // Error by default
 
    // We indicate that the channel isn't authenticated (use of
    // InsecureChannelCredentials()).
    client _client(
        grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));
    
    std::cout << "put" << "(" << test_key << ")" << ": " << test_value << std::endl;
    status = _client.kv739_put(test_key.c_str(), test_value.c_str(), old_value);
    std::cout << "status: " << status << " " << "put" << "(" << test_key << ")" << ": " << old_value << std::endl;
    std::cout << "----------- End PutTest -------------" << std::endl;
}


int main(int argc, char** argv) {
    absl::ParseCommandLine(argc, argv);
    // Instantiate the client. It requires a channel, out of which the actual RPCs
    // are created. This channel models a connection to an endpoint specified by
    // the argument "--target=" which is the only expected argument.
    std::string target_str = absl::GetFlag(FLAGS_target);
   
    runPutTest(target_str);
    runGetTest(target_str); 

    return 0;
}
