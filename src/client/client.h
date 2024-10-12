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

class client {
    public:
        client(std::shared_ptr<grpc::Channel> channel, int timeout, std::string _resp_server_addr);
        ~client();

        int get(std::string key, std::string &value);
        int put(std::string key, std::string value, std::string &old_value);

        uint16_t id;

        int timeout;

    private:
        std::string resp_server_addr;
        std::unique_ptr<kv_store::Stub> stub_;
        std::atomic<bool> rcvd_resp = false; //notify the main thread that the response server received response
        int status;
        std::string value;

        std::unique_ptr<grpc::Server> resp_server = nullptr;
        //std::unique_ptr<grpc::Server> start_response_server(std::string resp_server_addr);
        //void start_response_server(std::string resp_server_addr, std::atomic<bool>& stop);
        void start_response_server(std::string addr, std::unique_ptr<grpc::Server>& server);

        std::thread server_thread;
        std::atomic<bool> stop = true; // notify the sever to stop
};

class KVResponseService final : public KVResponse::Service {
    public:
        KVResponseService(std::string _addr, std::atomic<bool> *_rcvd_resp, int *_status, std::string *_value);

    private:
        std::string addr;
        std::atomic<bool> *rcvd_resp;
        int *status;
        std::string *value;

        grpc::Status sendGetResp(grpc::ServerContext* context, const getResp* get_resp, respStatus* resp_status) override;
        grpc::Status sendPutResp(grpc::ServerContext* context, const putResp* put_resp, respStatus* resp_status) override;
};

#endif  // client_h__
