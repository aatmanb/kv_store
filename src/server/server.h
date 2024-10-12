#pragma once

#include "dbutils.h"
#include <iostream>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_format.h"

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "kv_store.grpc.pb.h"
#include "node.grpc.pb.h"
#include "thread_safe_queue.h"

// ABSL_FLAG(uint16_t, port, 50051, "Server port for the service");

namespace key_value_store {
    void runServer(uint32_t id, const char *db_fname, bool head, bool tail, std::string addr, std::string prev_addr, std::string next_addr, std::string head_addr, std::string tail_addr);
    
    class kv_storeImpl final : public kv_store::Service {
    public:
        kv_storeImpl(uint32_t _id, const char *db_name, bool _head, bool _tail, std::string _addr, std::string _prev_addr, std::string _next_addr, std::string _head_addr, std::string _tail_addr); 
    
    private: 
        uint32_t id;
    
	    const char *db_name;

        bool tail;
        bool head;

        std::string addr;
        std::string prev_addr;
        std::string next_addr;
        std::string head_addr;
        std::string tail_addr;

        std::unique_ptr<kv_store::Stub> prev_stub = nullptr;
        std::unique_ptr<kv_store::Stub> next_stub = nullptr;
        std::unique_ptr<kv_store::Stub> head_stub = nullptr;
        std::unique_ptr<kv_store::Stub> tail_stub = nullptr;
       
        
        ThreadSafeQueue<int> pending_q; 
        
        std::string client_addr; // The client which send the request 
 
        grpc::Status get(grpc::ServerContext* context, const getReq* request, reqStatus* response) override;
        grpc::Status put(grpc::ServerContext* context, const putReq* request, reqStatus* response) override;
        
        
        std::unique_ptr<KVResponse::Stub> client_stub = nullptr;
        // Internal RPCs
        grpc::Status fwdGet(grpc::ServerContext* context, const fwdGetReq* request, empty* response) override;
        

        // TODO:
        //void pushToPendingQ();
    };

    //class Node final: public NodeService::Service {
    //public:
    //    Node(uint32_t _id, const std::string _addr, const std::string _prev_addr, const std::string _next_addr);
    //    connectPrev();
    //    connectNext(); 

    //private:
    //    uint32_t _id;

    //    std::string addr;
    //    std::string prev_add;
    //    std::string next_addr;

    //    std::unique_ptr<NodeService::Stub> prev_stub = nullptr;
    //    std::unique_ptr<NodeService::Stub> next_stub = nullptr;

    //    grpc::Status get(grpc::ServerContext* context, const node::getReq* req, node::getResp* resp) override;
    //    grpc::Status put(grpc::ServerContext* context, const node::putReq* req, node::putResp* resp) override;
    //    grpc::Status ack(grpc::ServerContext* context, const node::ackReq* req, node::ackResp* resp) override;
    //    
    //  
    //};

    //class Server {
    //public:
    //    Server(uint32_t _id, const char* db_name, const std::string _addr, const std::string _prev_addr, const std::string _next_addr);
    //    void Wait(); 

    //private:
    //    uint32_t id;

    //    kv_storeImpl service;
    //    Node node;
    //    //TODO:
    //    // shared queue to keep pending request
    //};

}
