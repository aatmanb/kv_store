#include <iostream>
#include <memory>
#include <string>
#include <cstring>
#include <stdexcept>
#include <cstdlib>
#include <chrono>
#include <string>
#include <mutex>
#include <condition_variable>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include <grpcpp/grpcpp.h>
#include "kv_store.grpc.pb.h"
#include "client.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;

client::client(int timeout, const std::string& config_file) : 
    id(0), 
    timeout(timeout)
{
    std::cout << "Parsing Chain Config file" << std::endl;

    partitions = parseConfigFile(config_file); 
    num_partitions = partitions.size(); 
    std::cout << "Number of partitions: " << num_partitions << std::endl;

    std::cout << "Establishing gRPC channels and setting up stubs" << std::endl;
    // establish a channel corresponding to each stub
    for (int i=0; i<num_partitions; i++) {
        std::string server_name = "localhost:" + partitions[i].getServer();
        std::cout << "Establishing channel with server " << server_name << std::endl;
        std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(server_name, grpc::InsecureChannelCredentials());
        //std::unique_ptr<kv_store::Stub> stub_(kv_store::NewStub(channel));
        std::cout << "Creating stub" << std::endl;
        stubs.push_back(std::move(kv_store::NewStub(channel)));
    }
     
    std::cout << "Starting response server" << std::endl;
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
    KVResponseService service(&rcvd_resp, &status, &value, &condVar);
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    
    // Finally assemble the server.
    server = std::move(builder.BuildAndStart());
   
    port = std::to_string(selected_port); 
    std::string selected_addr = "0.0.0.0:" + std::to_string(selected_port);
    
    std::cout << "response server listening on addr " << selected_addr << std::endl;
    started.store(true);
    server->Wait();
}

int
client::get(std::string key, std::string &value) {
    //std::string key_str = charArrayToString(key);
    //std::cout << "[client " << id << "] " << "get() called with key: " << key << std::endl;
    getReq request;
    request.set_key(key);
    auto *_meta = request.mutable_meta();
    _meta->set_addr("localhost:"+resp_server_addr);
    // request.set_id(id);

    reqStatus response;

    ClientContext context;
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(timeout);
    context.set_deadline(deadline);
    
    // std::unique_ptr<kv_store::Stub>& stub_ = getStub(key);
    // Status status = stub_->get(&context, request, &response);
    int num_retry = 0;

    while (num_retry < req_retry_limit) {
        std::cout << "REtry: " << num_retry << "\n";
        // Submit query
        std::unique_ptr<kv_store::Stub>& stub_ = getStub(key, num_retry);
        num_retry++;
        if (!stub_) {
            std::cout << "nullptr\n";
        }
        auto status = stub_->get(&context, request, &response);
        if (!status.ok()) continue;

        // Wait for response
        std::unique_lock<std::mutex> lock(lock_for_rcvd_resp);
        condVar.wait_for(lock, std::chrono::milliseconds(500), [this]{ return rcvd_resp.load(); });
        //std::this_thread::sleep_for(std::chrono::milliseconds(500));
        //if (!rcvd_resp.load()) {
            // Resubmit the query
            //continue;
        //}
        rcvd_resp.store(false);
        //std::cout << "response server passed the value to client: " << this->value << std::endl;
        value = this->value;
        return this->status;
    }

    // while (!status.ok() && (req_retry>0)) {
    //     // Send the request to a another server because previous rpc failed
    //     req_retry--;
    //     std::unique_ptr<kv_store::Stub>& stub_ = getStub(key, true);
    //     status = stub_->get(&context, request, &response);
    // }

    // if (status.ok()) {
    //     // TODO: resp_retry_limit
    //     std::cout << "waiting for server to respond: " << std::endl;
    //     while(!(rcvd_resp.load())) {
    //         //std::this_thread::sleep_for(std::chrono::milliseconds(100));
    //     }
    //     rcvd_resp.store(false);
    //     std::cout << "response server passed the value to client: " << this->value << std::endl;
    //     value = this->value;
    //     return this->status;
    // }

    // We will reach here if req_retry_limit was reached
    // std::cerr << __FILE__ << "[" << __LINE__ << "]" << status.error_message() << std::endl;
    std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Retries exhausted" << std::endl;
    return -1;
        
}

int
client::put(std::string key, std::string value, std::string &old_value) {
    ////std::string key_str = charArrayToString(key);
    ////std::string value_str = charArrayToString(value);
    //
    //std::cout << "[client " << id << "] " << "put" << "(" << key << ")" << ": " << value << std::endl;
    //
    putReq request;
    request.set_key(key);
    request.set_value(value);
    auto *_meta = request.mutable_meta();
    std::string addr = "localhost:"+resp_server_addr;
    _meta->set_addr(addr);

    reqStatus response;

    ClientContext context;
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(timeout);
    context.set_deadline(deadline);

    std::unique_ptr<kv_store::Stub>& stub_ = getStub(key);
    Status status = stub_->put(&context, request, &response);
    int req_retry = req_retry_limit;

    while (!status.ok() && (req_retry>0)) {
        // Send the request to a another server because previous rpc failed
        req_retry--;
        //std::unique_ptr<kv_store::Stub>& stub_ = getStub(key, true);
        status = stub_->put(&context, request, &response);
    }

    if (status.ok()) {
        // TODO: resp_retry_limit
        //std::cout << "waiting for server to respond: " << std::endl;
        while (!rcvd_resp.load()) {
             std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
        rcvd_resp.store(false);
        //std::cout << "response server passed the value to client: " << this->value << std::endl;
            old_value = this->value;
            return this->status;
    }
    std::cerr << __FILE__ << "[" << __LINE__ << "]" << status.error_message() << std::endl;
    return -1;
}

std::unique_ptr<kv_store::Stub> createStub(int port) {
    std::string server_name = "localhost:" + std::to_string(port);
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(server_name, grpc::InsecureChannelCredentials());
    return kv_store::NewStub(channel);
}

std::unique_ptr<kv_store::Stub> createStub(const std::string& server_name) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(server_name, grpc::InsecureChannelCredentials());
    return kv_store::NewStub(channel);
}

int client::kill(std::string server, int clean) {
    auto server_stub = createStub(server);
    grpc::ClientContext ctx;
    failCommand req;
    req.set_clean(clean);
    empty response;
    auto status = server_stub->fail(&ctx, req, &response);
    return status.ok() ? 0 : -1;
}

std::unique_ptr<kv_store::Stub>& 
client::getStub(const std::string& key, bool retry) {
    CustomHash hash;
    int partition_id = hash(key) % num_partitions;
    
    if (retry) {
        PartitionConfig partition = partitions[partition_id];
        std::unique_ptr<kv_store::Stub> stub_ = createStub(std::stoi(partition.getServer()));
        stubs[partition_id] = std::move(stub_);
    }

    return stubs[partition_id];
} 


KVResponseService::KVResponseService(std::atomic<bool> *_rcvd_resp, int *_status, std::string *_value, 
                                     std::condition_variable *_condVar):
    rcvd_resp(_rcvd_resp),
    status(_status),
    value(_value),
    condVar(_condVar)
{}

grpc::Status
KVResponseService::sendGetResp(grpc::ServerContext* context, const getResp* get_resp, respStatus* resp_status) {
    //std::cout << "received response for get" << std::endl;
    *status = get_resp->status();
    *value = get_resp->value();

    resp_status->set_status(0);
    *rcvd_resp = true;
    condVar->notify_one();

    return Status::OK; 
}

grpc::Status
KVResponseService::sendPutResp(grpc::ServerContext* context, const putResp* put_resp, respStatus* resp_status) {
    // TODO
    //std::cout << "received response for put" << std::endl;
    *status = put_resp->status();
    *value = put_resp->old_value();

    resp_status->set_status(0);
    *rcvd_resp = true;
    condVar->notify_one();
    return Status::OK;
}
