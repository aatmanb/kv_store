#pragma once

#include "dbutils.h"
#include "kv_store.grpc.pb.h"
// #include "node.grpc.pb.h"
#include "worker.h"
#include "thread_safe_queue.h"
#include "thread_safe_hashmap.h"
#include "request.h"
#include <iostream>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_format.h"

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

// ABSL_FLAG(uint16_t, port, 50051, "Server port for the service");

namespace key_value_store {

    void runServer(std::string &master_addr, std::string &local_addr);
    
    class kv_storeImpl2 final : public kv_store::Service {
    public:
        kv_storeImpl2(std::string &master_addr, std::string &addr); 
        ~kv_storeImpl2();

        /**
         * This function does the following:
         * - Informs manager about start/restart
         * - Opens connection to the database
         * - Launches threads
         * This is not part of the constructor since the grpc server should launch before we communicate with the manager
         */
        void start();
 
    private: 
        uint32_t id;
	const char *db_name;
        std::unique_ptr<DatabaseUtils> db_utils;

        std::atomic<bool> is_tail;
        std::atomic<bool> is_head;

        std::string addr;
        std::string manager_addr;
        std::string prev_addr;
        std::string next_addr;
        std::string head_addr;
        std::string tail_addr;

        std::unique_ptr<kv_store::Stub> prev_stub = nullptr;
        std::unique_ptr<kv_store::Stub> next_stub = nullptr;
        std::unique_ptr<kv_store::Stub> head_stub = nullptr;
        std::unique_ptr<kv_store::Stub> tail_stub = nullptr;
        std::unique_ptr<master::Stub> manager_stub = nullptr;
       
        ThreadSafeQueue<Request> pending_q; 
        ThreadSafeQueue<Request> sent_queue;
        ThreadSafeHashMap<std::string, std::string> local_map;
        
        std::string client_addr; // The client which send the request 
 
        grpc::Status get(grpc::ServerContext* context, const getReq* request, reqStatus* response) override;
        grpc::Status put(grpc::ServerContext* context, const putReq* request, reqStatus* response) override;
        grpc::Status fail(grpc::ServerContext* context, const failCommand* request, empty* response) override;
        
        std::unique_ptr<KVResponse::Stub> client_stub = nullptr;
        
        // Internal RPCs
        grpc::Status fwdGet(grpc::ServerContext* context, const fwdGetReq* request, empty* response) override;
        grpc::Status fwdPut(grpc::ServerContext *context, const fwdPutReq* request, empty *response) override;
        grpc::Status commit(grpc::ServerContext *context, const fwdPutReq* request, empty *response) override;
        grpc::Status ack(grpc::ServerContext *context, const putAck* request, empty *response) override;

        // Reconfiguration RPCs
        grpc::Status notifyPredFailure(grpc::ServerContext* context, 
                const notifyPredFailureReq* request, empty *response) override;
        grpc::Status notifySuccessorFailure(grpc::ServerContext* context, 
                const notifySuccessorFailureReq* request, empty *response) override;
        grpc::Status addTailNode(grpc::ServerContext *context, const addTailNodeReq *req, 
                empty* response) override;
        grpc::Status notifyHeadFailure(grpc::ServerContext* context,
                const headFailureNotification* request, empty *response) override;
        grpc::Status notifyTailFailure(grpc::ServerContext* context,
                const tailFailureNotification* request, empty *response) override;
        // grpc::Status populateDB(grpc::ServerContext *context, const empty *request, dbPath* response) override;

        // std::thread resp_thread; // This threads pops request from pending_q and sends response to the client. This is used by tail node only.
        Worker resp_thread;
        Worker get_thread;
        Worker put_thread;
        Worker commit_thread;
        Worker ack_thread;

        void serveRequest(Request &req);        
        void get_process(Request req);
        void put_process(Request req);
        void commit_process(Request req);
        void ack_process(Request req);
        /**
         * Resends lost updates in the case of failure of an intermediate node
         */
        void process_lost_updates();
        /**
         * Commits updates to db from sent queue after failure of tail
         */
        void commit_sent_updates();
    };
}
