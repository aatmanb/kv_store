#ifndef client_h__
#define client_h__

#include <iostream>
#include <memory>
#include <string>
#include <cstring>
#include <stdexcept>
#include <cstdlib>
#include <thread>
#include <condition_variable>
#include <atomic>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "kv_store.grpc.pb.h"
#include "utils.h"

class client {
public:
    client(int timeout, const std::string& config_file);
    ~client();

    int get(std::string key, std::string &value);
    int put(std::string key, std::string value, std::string &old_value);
    int kill(std::string server, int clean);

    // std::unique_ptr<kv_store::Stub> createStub(const std::string& port);
    std::unique_ptr<kv_store::Stub>& getStub(const std::string& key, bool retry=false);

    uint16_t id;

    int timeout;

private:
    std::string resp_server_addr;
    std::atomic<bool> resp_server_started;
    std::vector<std::unique_ptr<kv_store::Stub>> stubs;
    std::atomic<bool> rcvd_resp = false; //notify the main thread that the response server received response
    int status;
    std::string value;

    std::unique_ptr<grpc::Server> resp_server = nullptr;
    void start_response_server(std::unique_ptr<grpc::Server>& server, std::string& port, std::atomic<bool>& started);

    std::thread server_thread;

    std::vector<PartitionConfig> partitions;
    int num_partitions;

    std::unordered_map<std::string, int, CustomHash> key_to_partition;

    const int req_retry_limit = 5;
    const int resp_retry_limit = 5;
 
};

class KVResponseService final : public KVResponse::Service {
public:
    KVResponseService(std::atomic<bool> *_rcvd_resp, int *_status, std::string *_value);

private:
    std::string addr;
    std::atomic<bool> *rcvd_resp;
    int *status;
    std::string *value;

    grpc::Status sendGetResp(grpc::ServerContext* context, const getResp* get_resp, respStatus* resp_status) override;
    grpc::Status sendPutResp(grpc::ServerContext* context, const putResp* put_resp, respStatus* resp_status) override;
};

#endif  // client_h__
