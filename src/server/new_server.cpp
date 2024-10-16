#include "new_server.h"
#include "data.h"
#include "partition_manager.h"

#define KV_FAILURE -1
#define KV_GET_SUCCESS 0
#define KV_GET_FAILED  1
#define KV_UPDATE_SUCCESS 2
#define KV_PUT_SUCCESS 3
#define KV_PUT_RECEIVED 4
#define KV_PUT_REDIRECT 5

#define MAX_KEY_LEN 128
#define MAX_VALUE_LEN 2048

using grpc::Channel;
using grpc::ClientContext;
using grpc::ServerContext;
using grpc::Status;

namespace key_value_store {
    void runServer(std::string &master_addr, std::string &local_addr) {
        kv_storeImpl2 service(master_addr, local_addr);
        
        auto part_mgr = PartitionManager::get_instance();
        
        // part_mgr->set_db_path_directory(db_fname);
        part_mgr->create_partitions();
        
        grpc::EnableDefaultHealthCheckService(true);
        grpc::reflection::InitProtoReflectionServerBuilderPlugin();
        grpc::ServerBuilder builder;
        // Listen on the given address without any authentication mechanism.
        builder.AddListeningPort(local_addr, grpc::InsecureServerCredentials());
        // Register "service" as the instance through which we'll communicate with
        // clients. In this case it corresponds to an *synchronous* service.
        builder.RegisterService(&service);
        // Finally assemble the server.
        std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
        std::cout << "Server listening on " << local_addr << std::endl;
        
        // Wait for the server to shutdown. Note that some other thread must be
        // responsible for shutting down the server for this call to ever return.
        server->Wait();
    }

    kv_storeImpl2::kv_storeImpl2(std::string &master_addr, std::string &addr): 
            manager_addr(master_addr),
            addr(addr) {
        is_tail.store(true);
        stop.store(false);

        if (!manager_addr.empty()) {
            manager_stub = master::Service(grpc::CreateChannel(manager_addr, grpc::InsecureChannelCredentials()));
        } else {
            // Manager address is empty
            throw new std::runtime_error("No manager address provided");
        }
        grpc::ClientContext ctx;
        notifyRestartReq req;
        req.set_node(addr);
        empty response;
        auto status = manager_stub->notifyRestart(&ctx, req, &response);
        if (!status.ok()) {
            throw new std::runtime_error(status.error_message());
        }

        if (!prev_addr.empty()) {
            prev_stub = kv_store::NewStub(grpc::CreateChannel(prev_addr, grpc::InsecureChannelCredentials()));
        }
        if (!next_addr.empty()) {
            next_stub = kv_store::NewStub(grpc::CreateChannel(next_addr, grpc::InsecureChannelCredentials()));
        }
        if (!is_head) {
            head_stub = kv_store::NewStub(grpc::CreateChannel(head_addr, grpc::InsecureChannelCredentials()));
        }
        if (!is_tail) {
            tail_stub = kv_store::NewStub(grpc::CreateChannel(tail_addr, grpc::InsecureChannelCredentials()));
        }

        ack_thread.start();
        commit_thread.start();
        get_thread.start();
        put_thread.start();
        if (is_tail) {
            resp_thread.start();
        }
    }

    kv_storeImpl2::~kv_storeImpl2() {
        ack_thread.pause();
        commit_thread.pause();
        get_thread.pause();
        put_thread.pause();
        resp_thread.pause();
    }

    grpc::Status kv_storeImpl2::get(ServerContext* context, const getReq* request, reqStatus* response) {
        std::cout << "id: " << id <<  " GET CALLED!!" << std::endl;
        Request req = Request(*request);
        try {
            get_thread.post(std::bind(&kv_storeImpl2::get_process, this, req));
            response->set_status(KV_GET_SUCCESS);
        } catch (const std::exception& e) {
            std::cerr << "Error in get_process: " << e.what() << std::endl;
            return grpc::Status(grpc::StatusCode::INTERNAL, "Internal error");
        }
        return Status::OK;
    }

    void kv_storeImpl2::get_process(Request req) {
        if (is_tail.load()) {
            resp_thread.post(std::bind(&kv_storeImpl2::serveRequest, req));
        } else {
	        std::cout << "forwarding to tail" << std::endl;
            ClientContext _context;
            fwdGetReq _req = req.rpc_fwdGetReq();
            empty _resp;
            Status status = tail_stub->fwdGet(&_context, _req, &_resp);
        }
    }

    grpc::Status kv_storeImpl2::put(grpc::ServerContext* context, const putReq* request, reqStatus* response) {	
	    std::cout << "id: " << id << " PUT CALLED!!\n";
        Request req = Request(*request);

        try {
            put_thread.post(std::bind(&kv_storeImpl2::put_process, this, req));
            response->set_status(KV_PUT_RECEIVED);
        } catch (const std::exception& e) {
            std::cerr << "Error in get_process: " << e.what() << std::endl;
            return grpc::Status(grpc::StatusCode::INTERNAL, "Internal error");
        }
        return Status::OK;
    }

    grpc::Status kv_storeImpl2::fail(grpc::ServerContext* context, const failCommand* request, reqStatus* response) {
        bool clean = request->clean();
        if (clean) {
            grpc::ClientContext ctx;
            notifyFailureReq req;
            req.set_failednode(addr);
            empty response;
            manager_stub->notifyFailure(&ctx, req, &response);
        }
        exit();
        return grpc::OK();
    }

    void kv_storeImpl2::put_process(Request req) {
	    if (is_head.load()) {
	        std::cout << "HEAD: " << head << ", received put() request\n";
            commit_thread.post(std::bind(&kv_storeImpl2::commit_process, this, req));
        }
        else {
            // TODO
	        // Acknowledge that we received the PUT request
	        // TODO: response->set_status(KV_PUT_RECEIVED);
	        // TODO: Store it in the db here
	        // Prepare the forwarding request
	        ClientContext _context;
            fwdPutReq _req = req.rpc_fwdPutReq();
	        //fwdPutReq _req;
	        //auto *original_req = _req.mutable_req();
	        //auto *meta = original_req->mutable_meta();
	        //original_req->set_key (req.key);
	        //original_req->set_value (req.value);
	        //meta->set_addr(req.addr);
	        empty _resp;
	        Status status = head_stub->fwdPut(&_context, _req, &_resp);
        }
    }
    
    grpc::Status kv_storeImpl2::fwdGet(ServerContext* context, const fwdGetReq* request, empty* response) {
        std::cout << "id: " << id <<  " received fwdGetReq" << std::endl;
        // TODO(): This assertion could fail during reconfiguration
        assert(is_tail.load()); // Only tail shoudl receive forwarded getReq
        std::cout << "pushing to pending Q" << std::endl;
        resp_thread.post(std::bind(&kv_storeImpl2::serveRequest, req));
        return Status::OK;
    }

    grpc::Status kv_storeImpl2::fwdPut(ServerContext* context, const fwdPutReq* request, empty* response) {
	    std::cout << "id: " << id << " received fwdPutReq\n";
        assert(is_head.load());
	    Request req = Request(*request);
        commit_thread.post(std::bind(&kv_storeImpl2::commit_process, this, req));
	    return Status::OK;
    }

    grpc::Status kv_storeImpl2::commit(ServerContext* context, const fwdPutReq* request, empty* response) {
	    std::cout << "id: " << id << " received commit\n";
        assert(!is_head.load());
        Request req = Request(*request);
        commit_thread.post(std::bind(&kv_storeImpl2::commit_process, this, req));
        return Status::OK;
    }
    
    grpc::Status kv_storeImpl2::ack(ServerContext* context, const empty* request, empty* response) {
	    std::cout << "id: " << id << " received ack\n";
        assert(!is_tail.load());
        ack_thread.post(std::bind(&kv_storeImpl2::ack_process, this));
        return Status::OK;
    }

    grpc::Status kv_storeImpl2::notifyPredFailure(grpc::ServerContext* context, 
                const notifyPredFailureReq* request, empty *response) {
        bool was_head = request->washead();
        ack_thread.pause();
        
        if (was_head) {
            // Head has failed. Make current node the new head
            put_thread.pause();
            // Modify stubs
            prev_stub.reset();
            head_stub.reset();

            // Modify addresses
            prev_addr.clear();
            head_addr.clear();

            is_head.store(true);
            std::cout << "Successfully changed head to current node\n";
        } else {
            prev_addr = request->newpred();
            prev_stub = kv_store::NewStub(grpc::CreateChannel(prev_addr, grpc::InsecureChannelCredentials()));
        }
        put_thread.start();
        ack_thread.start();
        return grpc::OK;
    }

    grpc::Status kv_storeImpl2::notifySuccessorFailure(grpc::ServerContext* context, 
                const notifySuccessorFailureReq* request, empty *response) {
        std::string new_successor = request->newsuccessor();
        bool was_tail = request->wastail();
        ack_thread.pause();
        commit_thread.pause();
        if (was_tail) {
            std::cout << "Processing tail failure at predecessor\n";

            // Modify stubs
            next_stub.reset();
            tail_stub.reset();

            // Modify addresses
            next_addr.clear();
            tail_addr.clear();

            is_tail.store(true);
            resp_thread.start();
        } else {
            // Modify address and stub for new successor
            next_addr = request->newsuccessor();
            next_stub.reset();
            next_stub = kv_store::NewStub(grpc::CreateChannel(next_addr, grpc::InsecureChannelCredentials()));

            // Notify new successor that its predecessor has failed
            ClientContext ctx;
            notifyPredFailureReq req;
            req.set_newpred(addr);
            req.set_washead(false);
            empty resp;
            next_stub->notifyPredFailure(&ctx, req, &resp);

            process_lost_updates();
        }
        commit_thread.start();
        ack_thread.start();
        return grpc::OK;
    }

    grpc::Status kv_storeImpl2::addTailNode(grpc::ServerContext *context, const addTailNodeReq *req, 
                empty* response) {
        commit_thread.pause();
        put_thread.pause();
        if (is_tail.load()) {
            is_tail.store(false);
            
        }

    }

    grpc::Status kv_storeImpl2::populateDB(grpc::ServerContext *context, const empty *request, 
            dbPath* response) {
        
    }

    void kv_storeImpl2::process_lost_updates() {
        ThreadSafeQueue<Request> tmp_queue;
        while (true) {
            auto val = sent_queue.tryDequeue();
            if (val == std::nullopt) {
                break;
            }
            ClientContext _context;
            fwdPutReq _req = val.value().rpc_fwdPutReq();
            empty _resp;
            next_stub->commit(&_context, _req, &_resp);
            tmp_queue.enqueue(val.value());
        }
        sent_queue = std::move(tmp_queue);
    }

    void kv_storeImpl2::commit_process(Request req) {
        // TODO:
        // 1. Commit to own database
        if (!is_tail.load()) {
            /**
             * Break this into 2 steps:
             * - Thread 1: Write to database/hashmap, append to to_send queue
             * - Thread 2: Forward commit from to_send queue, append to sent queue
             */
	        ClientContext _context;
            fwdPutReq _req = req.rpc_fwdPutReq();
            empty _resp;
            next_stub->commit(&_context, _req, &_resp);
            sent_queue.enqueue(req);
        }
        else {
            std::cout << "tail receive commit from " << req.addr << std::endl;
            resp_thread.post(std::bind(&kv_storeImpl2::serveRequest, this, req));
        }
    }

    void kv_storeImpl2::ack_process() {
        // TODO:
        // 1. delete entry to sent_q
        sent_queue.dequeue();
        if (!is_head) {
	        ClientContext _context;
            empty _req;
            empty _resp;
            prev_stub->ack(&_context, _req, &_resp); 
        }
        else {
            std::cout << "head received ack" << std::endl;
        }
    }

    void kv_storeImpl2::serveRequest(Request &req) {
        std::string client_addr = req.addr;
        std::cout << "client_addr: " << client_addr << std::endl;
        client_stub = KVResponse::NewStub(grpc::CreateChannel(client_addr, grpc::InsecureChannelCredentials()));
        ClientContext _context;
        respStatus _resp;

        if (req.type == request_t::GET) {
            // TODO(): Add db code
            std::cout << "Processing client get() request" << std::endl;
            getResp _req;
            _req.set_value("Get call successful");
            _req.set_status(KV_GET_SUCCESS);
            
            // Send response to client
            Status status = client_stub->sendGetResp(&_context, _req, &_resp);
            // Send ack to predecessor
            std::cout << "Sent get response to client" << std::endl;
        } else if (req.type == request_t::PUT) {
            // TODO(): Add db code
            std::cout << "Processing client put() request" << std::endl;
            putResp _req;
            _req.set_old_value("Put call successful");
            _req.set_status(KV_PUT_SUCCESS);

            // Send response to client
            Status status = client_stub->sendPutResp(&_context, _req, &_resp);
            // Send ack to predecessor
            ack_thread.post(std::bind(&kv_storeImpl2::ack_process, this));
            std::cout << "Sent put response to client" << std::endl;
        } else {
            std::cerr << "Invalid request type" << std::endl;
            std::exit(1);
        }
    }
}
