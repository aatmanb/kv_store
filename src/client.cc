#include <iostream>
#include <memory>
#include <string>
#include <cstring>
#include <stdexcept>
#include <cstdlib>
#include <chrono>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include <grpcpp/grpcpp.h>
#include "kv_store.grpc.pb.h"
#include "client.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

client::client(std::shared_ptr<Channel> channel, int timeout) : stub_(kv_store::NewStub(channel)), id(0), timeout(timeout) {}

int
client::get(std::string key, std::string &value) {
    //std::string key_str = charArrayToString(key);
    //std::cout << "[client " << name << "] " << "get() called with key: " << key_str << std::endl;
    getReq request;
    request.set_key(key);
    // request.set_name(id);

    getResp response;

    ClientContext context;
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(timeout);
    context.set_deadline(deadline);
    
    Status status = stub_->get(&context, request, &response);
    if (status.ok()) {
        value = response.value();
        return response.status();
    }
    std::cerr << __FILE__ << "[" << __LINE__ << "]" << status.error_message() << std::endl;
    return -1;
        
}

int
client::put(std::string key, std::string value, std::string &old_value) {
    //std::string key_str = charArrayToString(key);
    //std::string value_str = charArrayToString(value);
    
    //std::cout << "[client " << name << "] " << "put" << "(" << key_str << ")" << ": " << value_str << std::endl;
    
    putReq request;
    request.set_key(key);
    request.set_value(value);
    // request.set_name(id);

    putResp response;

    ClientContext context;
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(timeout);
    context.set_deadline(deadline);

    Status status = stub_->put(&context, request, &response);
    if (status.ok()) {
        old_value = response.old_value();
        return response.status();
    }
    std::cerr << __FILE__ << "[" << __LINE__ << "]" << status.error_message() << std::endl;
    return -1;
}
