#include <iostream>
#include <memory>
#include <string>
#include <cstring>
#include <stdexcept>
#include <cstdlib>
#include <chrono>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include <grpcpp/grpcpp.h>
#include "kv_store.grpc.pb.h"
#include "client.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

client::client(std::shared_ptr<Channel> channel, int timeout) : 
    stub_(kv_store::NewStub(channel)), 
    id(0), 
    timeout(timeout)
{
    // Spawn two threads
    // thread 0: run the server
    // thread 1: continue with client construction
    rcvd_resp.store(false);
    resp_server_started.store(false);
    server_thread = std::thread(&client::start_response_server, this, std::ref(resp_server), std::ref(resp_server_addr), std::ref(resp_server_started));
    while (!resp_server_started.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::cout << "client " << id << " started response server" << std::endl;
}

client::~client() {
    // TODO: 
    // 1. close the response server to tail connection
    // 2. kill the response server
    // 3. close the client to CR server conection
    std::cout << "killing server" << std::endl;
    resp_server->Shutdown();
    if (server_thread.joinable()) {
        server_thread.join();
    }
}

void
client::start_response_server(std::unique_ptr<grpc::Server>& server, std::string& port, std::atomic<bool>& started) {
    std::string addr = "0.0.0.0:0";
    
    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    grpc::ServerBuilder builder;
    int selected_port;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials(), &selected_port);
    KVResponseService service(&rcvd_resp, &status, &value);
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    
    // Finally assemble the server.
    server = std::move(builder.BuildAndStart());
   
    port = std::to_string(selected_port); 
    std::string selected_addr = "0.0.0.0:" + std::to_string(selected_port);
    
    std::cout << "response server listening on selected_port " << selected_port << std::endl;
    std::cout << "response server listening on port " << port << std::endl;
    std::cout << "response server listening on addr " << selected_addr << std::endl;
    started.store(true);
    server->Wait();
}

int
client::get(std::string key, std::string &value) {
    //std::string key_str = charArrayToString(key);
    std::cout << "[client " << id << "] " << "get() called with key: " << key << std::endl;
    getReq request;
    request.set_key(key);
    auto *_meta = request.mutable_meta();
    _meta->set_addr("localhost:"+resp_server_addr);
    // request.set_id(id);

    reqStatus response;

    ClientContext context;
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(timeout);
    context.set_deadline(deadline);
    
    Status status = stub_->get(&context, request, &response);
    if (status.ok()) {
        std::cout << "waiting for server to respond: " << std::endl;
        while(!(rcvd_resp.load())) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        rcvd_resp.store(false);
        std::cout << "response server passed the value to client: " << this->value << std::endl;
        value = this->value;
        return this->status;
    }
    std::cerr << __FILE__ << "[" << __LINE__ << "]" << status.error_message() << std::endl;
    return -1;
        
}

int
client::put(std::string key, std::string value, std::string &old_value) {
    ////std::string key_str = charArrayToString(key);
    ////std::string value_str = charArrayToString(value);
    //
    std::cout << "[client " << id << "] " << "put" << "(" << key << ")" << ": " << value << std::endl;
    //
    putReq request;
    request.set_key(key);
    request.set_value(value);
    auto *_meta = request.mutable_meta();
    std::string addr = "localhost:"+resp_server_addr;
    std::cout << "addr " << addr << std::endl;
    _meta->set_addr(addr);

    reqStatus response;

    ClientContext context;
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(timeout);
    context.set_deadline(deadline);

    Status status = stub_->put(&context, request, &response);
//    std::cout << "Put called to server" << std::endl;
//    std::cout << "Status Code: " << status.error_code() << std::endl; std::cout << "Error Message: " << status.error_message() << std::endl;
    if (status.ok()) {
        std::cout << "waiting for server to respond: " << std::endl;
        while (!rcvd_resp.load()) {
             std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        rcvd_resp.store(false);
        std::cout << "response server passed the value to client: " << this->value << std::endl;
            old_value = this->value;
            return this->status;
        }
    //std::cerr << __FILE__ << "[" << __LINE__ << "]" << status.error_message() << std::endl;
    return -1;
}


KVResponseService::KVResponseService(std::atomic<bool> *_rcvd_resp, int *_status, std::string *_value):
    rcvd_resp(_rcvd_resp),
    status(_status),
    value(_value)
{}

grpc::Status
KVResponseService::sendGetResp(grpc::ServerContext* context, const getResp* get_resp, respStatus* resp_status) {
    std::cout << "received response for get" << std::endl;
    *status = get_resp->status();
    *value = get_resp->value();

    resp_status->set_status(0);
    *rcvd_resp = true;

    return Status::OK; 
}

grpc::Status
KVResponseService::sendPutResp(grpc::ServerContext* context, const putResp* put_resp, respStatus* resp_status) {
    // TODO
    std::cout << "received response for put" << std::endl;
    *status = put_resp->status();
    *value = put_resp->old_value();

    resp_status->set_status(0);
    *rcvd_resp = true;
    return Status::OK;
}
