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
#include "client.h"
#include "client_utils.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

client::client(std::shared_ptr<Channel> channel) : stub_(kv_store::NewStub(channel)), id(0) {}

int
client::get(std::string key, std::string &value) {
    //std::string key_str = charArrayToString(key);
    //std::cout << "[client " << name << "] " << "get() called with key: " << key_str << std::endl;
    getReq request;
    request.set_key(key);
    // request.set_name(id);

    getResp response;

    ClientContext context;

    Status status = stub_->get(&context, request, &response);
    value = response.value();
    //std::strcpy(value, stringToCharArray(response.value()));
    //std::cout << "[client " << name << "] " << "response for get" << "(" << key << ")" << ": " << value << std::endl;
    return response.status();
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

    Status status = stub_->put(&context, request, &response);
    old_value = response.old_value();
    //std::strcpy(old_value, stringToCharArray(response.old_value()));
    //std::cout << "[client " << name << "] " << "response for put" << "(" << key << ")" << ": " << old_value << std::endl;
    return response.status();
}