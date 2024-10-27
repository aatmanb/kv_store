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

client::client(int _id, int timeout, const std::string& config_file, const std::string& log_dir) : 
    id(_id), 
    timeout(timeout)
{
    std::string log_file_name = log_dir + "spdlog_client_" + std::to_string(id) + ".log";
    COUT << log_file_name << std::endl;
    
    // Logging example
    spdlog::flush_every(std::chrono::milliseconds(1));
    logger = spdlog::basic_logger_mt("basic_logger", log_file_name);
    // Set the logging level
    logger->set_level(spdlog::level::debug);
    logger->flush_on(spdlog::level::debug);
    SPDLOG_LOGGER_TRACE(logger , "Some trace message that will be evaluated.{} ,{}", 1, 3.23);
    SPDLOG_LOGGER_DEBUG(logger , "Some Debug message that will be evaluated.. {} ,{}", 1, 3.23);
    SPDLOG_LOGGER_INFO(logger , "Some Info message that will be evaluated.. {} ,{}", 1, 3.23);
    SPDLOG_LOGGER_WARN(logger , "Some Warn message that will be evaluated.. {} ,{}", 1, 3.23);
    SPDLOG_LOGGER_ERROR(logger , "Some Error message that will be evaluated.. {} ,{}", 1, 3.23);
    SPDLOG_LOGGER_CRITICAL(logger , "Some Critical message that will be evaluated.. {} ,{}", 1, 3.23);

    SPDLOG_LOGGER_INFO(logger , "parsing config file");
    partitions = parseConfigFile(config_file); 
    num_partitions = partitions.size(); 
    SPDLOG_LOGGER_INFO(logger , "number of partitions: ", num_partitions);
    std::cout << "Number of partitions: " << num_partitions << std::endl;

    SPDLOG_LOGGER_INFO(logger , "Establishing gRPC channels and setting up stubs");
    std::cout << "Establishing gRPC channels and setting up stubs" << std::endl;
    // establish a channel corresponding to each stub
    for (int i=0; i<num_partitions; i++) {
        std::string addr = "localhost:" + partitions[i].getServer();
        SPDLOG_LOGGER_INFO(logger , "establishing channel with server {}" , addr);
        std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
        SPDLOG_LOGGER_TRACE(logger , "creating stub");
        server_configs.push_back(new ServerConfig(addr, kv_store::NewStub(channel)));
    }
     
    SPDLOG_LOGGER_INFO(logger , "Starting response server");
    std::cout << "Starting response server" << std::endl;
    // Spawn two threads
    // thread 0: run the server
    // thread 1: continue with client construction
    rcvd_resp.store(false);
    resp_server_started.store(false);
    server_thread = std::thread(&client::start_response_server, this, std::ref(resp_server), std::ref(resp_server_addr), std::ref(resp_server_started), logger);
    while (!resp_server_started.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    SPDLOG_LOGGER_INFO(logger , "started response server");
    std::cout << "client " << id << " started response server" << std::endl;
}

client::~client() {
    SPDLOG_LOGGER_INFO(logger , "killing server");
    std::cout << "killing server" << std::endl;
    resp_server->Shutdown();
    if (server_thread.joinable()) {
        server_thread.join();
    }
}

void
client::start_response_server(std::unique_ptr<grpc::Server>& server, std::string& port, std::atomic<bool>& started, std::shared_ptr<spdlog::logger> logger) {
    std::string addr = "0.0.0.0:0";
    
    grpc::EnableDefaultHealthCheckService(true);
    grpc::reflection::InitProtoReflectionServerBuilderPlugin();
    grpc::ServerBuilder builder;
    int selected_port;
    // Listen on the given address without any authentication mechanism.
    builder.AddListeningPort(addr, grpc::InsecureServerCredentials(), &selected_port);
    KVResponseService service(&rcvd_resp, &status, &value, &condVar, logger);
    // Register "service" as the instance through which we'll communicate with
    // clients. In this case it corresponds to an *synchronous* service.
    builder.RegisterService(&service);
    
    // Finally assemble the server.
    server = std::move(builder.BuildAndStart());
   
    port = std::to_string(selected_port); 
    std::string selected_addr = "0.0.0.0:" + std::to_string(selected_port);
    
    SPDLOG_LOGGER_INFO(logger , "response server listening on addr ", selected_addr);
    std::cout << "response server listening on addr " << selected_addr << std::endl;
    started.store(true);
    server->Wait();
}

int
client::get(std::string key, std::string &value) {
    getReq request;
    request.set_key(key);
    auto *_meta = request.mutable_meta();
    _meta->set_addr("localhost:"+resp_server_addr);

    reqStatus response;

    ClientContext context;
    auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(timeout);
    context.set_deadline(deadline);
    
    int num_retry_per_server, num_retry_per_key;
    ServerConfig *server;

    num_retry_per_key = 0;
    while (num_retry_per_key < req_retry_limit_per_key) {
        // Always try a new server for better load distribution
        server = getStub(key, true);
        num_retry_per_key++;
        SPDLOG_LOGGER_INFO(logger, "Connecting to server {} for key {}", server->addr, key);
        num_retry_per_server = 0;
        while (num_retry_per_server < req_retry_limit_per_server) {
            // Submit query
            num_retry_per_server++;
            auto status = server->stub->get(&context, request, &response);
            if (!status.ok()) {
                SPDLOG_LOGGER_WARN(logger , "Couldn't send request. RPC timeout {}s", timeout);
                continue;
            }

            SPDLOG_LOGGER_DEBUG(logger , "Sent get() for key {}", key);

            // Wait for response
            std::unique_lock<std::mutex> lock(lock_for_rcvd_resp);
            condVar.wait_for(lock, std::chrono::milliseconds(500), [this]{ return rcvd_resp.load(); });
            if (rcvd_resp.load()) {
                rcvd_resp.store(false);
                value = this->value;
                return this->status;
            }
            else {
                SPDLOG_LOGGER_WARN(logger, "Response timeout {}ms", 500);
            }
        }
        SPDLOG_LOGGER_WARN(logger , "Retries limit reached for server {}", server->addr);
    }

    SPDLOG_LOGGER_CRITICAL(logger , "Retries limit reached for all servers in config. We should never see this!!");
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

    int num_retry_per_server, num_retry_per_key;
    ServerConfig *server;

    num_retry_per_key = 0;
    while (num_retry_per_key < req_retry_limit_per_key) {
        // Always try a new server for better load distribution
        server = getStub(key, true);
        num_retry_per_key++;
        SPDLOG_LOGGER_INFO(logger, "Connecting to server {} for key {}", server->addr, key);
        num_retry_per_server = 0;
        while (num_retry_per_server < req_retry_limit_per_server) {
            // Submit query
            num_retry_per_server++;
            auto status = server->stub->put(&context, request, &response);
            if (!status.ok()) {
                SPDLOG_LOGGER_WARN(logger , "Couldn't send request. RPC timeout {}s", timeout);
                continue;
            }

            SPDLOG_LOGGER_DEBUG(logger , "Sent put() for key {}", key);

            // Wait for response
            std::unique_lock<std::mutex> lock(lock_for_rcvd_resp);
            condVar.wait_for(lock, std::chrono::milliseconds(500), [this]{ return rcvd_resp.load(); });
            if (rcvd_resp.load()) {
                rcvd_resp.store(false);
                value = this->value;
                return this->status;
            }
            else {
                SPDLOG_LOGGER_WARN(logger, "Response timeout {}ms", 500);
            }
        }
        SPDLOG_LOGGER_WARN(logger , "Retries limit reached for server {}", server->addr);
    }

    SPDLOG_LOGGER_CRITICAL(logger , "Retries limit reached for all servers in config. We should never see this!!");
    return -1;
}

ServerConfig* 
client::createStub(int port) {
    std::string addr = "localhost:" + std::to_string(port);
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    return new ServerConfig(addr, kv_store::NewStub(channel));
}

ServerConfig*
client::createStub(const std::string& addr) {
    std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(addr, grpc::InsecureChannelCredentials());
    return new ServerConfig(addr, kv_store::NewStub(channel));
}

int client::kill(std::string server, int clean) {
    //auto server_stub = createStub(server);
    //grpc::ClientContext ctx;
    //failCommand req;
    //req.set_clean(clean);
    //empty response;
    //auto status = server_stub->fail(&ctx, req, &response);
    //return status.ok() ? 0 : -1;
    return -1;
}

ServerConfig*
client::getStub(const std::string& key, bool retry) {
    CustomHash hash;
    int partition_id = hash(key) % num_partitions;
    
    if (retry) {
        delete server_configs[partition_id];
        PartitionConfig partition = partitions[partition_id];
        ServerConfig *config = createStub(std::stoi(partition.getServer()));
        server_configs[partition_id] = config;
    }

    return server_configs[partition_id];
} 


KVResponseService::KVResponseService(std::atomic<bool> *_rcvd_resp, int *_status, std::string *_value, std::condition_variable *_condVar, std::shared_ptr<spdlog::logger> _logger):
    rcvd_resp(_rcvd_resp),
    status(_status),
    value(_value),
    condVar(_condVar),
    logger(_logger)
{}

grpc::Status
KVResponseService::sendGetResp(grpc::ServerContext* context, const getResp* get_resp, respStatus* resp_status) {
    SPDLOG_LOGGER_DEBUG(logger, "response server received get response");
    *status = get_resp->status();
    *value = get_resp->value();

    resp_status->set_status(0);
    *rcvd_resp = true;
    condVar->notify_one();

    return Status::OK; 
}

grpc::Status
KVResponseService::sendPutResp(grpc::ServerContext* context, const putResp* put_resp, respStatus* resp_status) {
    SPDLOG_LOGGER_DEBUG(logger, "response server received put response");
    *status = put_resp->status();
    *value = put_resp->old_value();

    resp_status->set_status(0);
    *rcvd_resp = true;
    condVar->notify_one();
    return Status::OK;
}
