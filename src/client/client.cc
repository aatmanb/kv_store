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

client::client(std::shared_ptr<Channel> channel, int timeout, std::string _resp_server_addr) : 
    stub_(kv_store::NewStub(channel)), 
    id(0), 
    timeout(timeout),
    resp_server_addr(_resp_server_addr)
{
    // Spawn two threads
    // thread 0: run the server
    // thread 1: continue with client construction
    //std::thread server_thread(start_response_server, resp_server_addr);
    resp_server = start_response_server(resp_server_addr);
    std::cout << "client " << id << " started response server" << std::endl;
    
}

client::~client() {
    resp_server->Shutdown();
    // TODO: 
    // 1. close the response server to tail connection
    // 2. kill the response server
    // 3. close the client to CR server conection
}

std::unique_ptr<grpc::Server>
client::start_response_server(std::string addr) {
    KVResponseService service(addr, &rcvd_resp, &status, &value);
    
    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    grpc::ServerBuilder builder;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials());
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    // Finally assemble the server.
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    std::cout << "Server listening on " << addr << std::endl;
    
    return server;
   
    //while(!stop) {
    //    std::this_thread::sleep_for(std::chrono::seconds(1));
    //}

 
    //// Wait for the server to shutdown. Note that some other thread must be
    //// responsible for shutting down the server for this call to ever return.
    //server->Wait();
}

int
client::get(std::string key, std::string &value) {
    //std::string key_str = charArrayToString(key);
    std::cout << "[client " << id << "] " << "get() called with key: " << key << std::endl;
    getReq request;
    request.set_key(key);
    // request.set_id(id);

    reqStatus response;

    ClientContext context;
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(timeout);
    context.set_deadline(deadline);
    
    Status status = stub_->get(&context, request, &response);
    if (status.ok()) {
        while(!(rcvd_resp)) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
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
    ////std::cout << "[client " << id << "] " << "put" << "(" << key_str << ")" << ": " << value_str << std::endl;
    //
    //putReq request;
    //request.set_key(key);
    //request.set_value(value);
    //// request.set_id(id);

    //putResp response;

    //ClientContext context;
    //auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(timeout);
    //context.set_deadline(deadline);

    //Status status = stub_->put(&context, request, &response);
    //if (status.ok()) {
    //    old_value = response.old_value();
    //    return response.status();
    //}
    //std::cerr << __FILE__ << "[" << __LINE__ << "]" << status.error_message() << std::endl;
    return -1;
}


KVResponseService::KVResponseService(std::string _addr, std::atomic<bool> *_rcvd_resp, int *_status, std::string *_value):
    addr(_addr),
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
    return Status::OK;
}
