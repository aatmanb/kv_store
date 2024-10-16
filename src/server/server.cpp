#include "server.h"
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
    void runServer(uint32_t id, const char *db_fname, bool head, bool tail, std::string addr, std::string prev_addr, std::string next_addr, std::string head_addr, std::string tail_addr) {
        kv_storeImpl service(id, db_fname, head, tail, addr, prev_addr, next_addr, head_addr, tail_addr);
        
        auto part_mgr = PartitionManager::get_instance();
        
        part_mgr->set_db_path_directory(db_fname);
        part_mgr->create_partitions();
        
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
        
        // Wait for the server to shutdown. Note that some other thread must be
        // responsible for shutting down the server for this call to ever return.
        server->Wait();
    }

    kv_storeImpl::kv_storeImpl(uint32_t _id, const char *db_name, bool _head, bool _tail, std::string _addr, std::string _prev_addr, std::string _next_addr, std::string _head_addr, std::string _tail_addr): 
        id(_id),
        db_name(db_name),
        head(_head),
        tail(_tail),
        addr(_addr),
        prev_addr(_prev_addr),
        next_addr(_next_addr),
        head_addr(_head_addr),
        tail_addr(_tail_addr)
    {
        stop.store(false);

        if (!prev_addr.empty()) {
            prev_stub = kv_store::NewStub(grpc::CreateChannel(prev_addr, grpc::InsecureChannelCredentials()));
        }
        if (!next_addr.empty()) {
            next_stub = kv_store::NewStub(grpc::CreateChannel(next_addr, grpc::InsecureChannelCredentials()));
        }
        if (!head) {
            head_stub = kv_store::NewStub(grpc::CreateChannel(head_addr, grpc::InsecureChannelCredentials()));
        }
        if (!tail) {
            tail_stub = kv_store::NewStub(grpc::CreateChannel(tail_addr, grpc::InsecureChannelCredentials()));
        }

        if (tail) {
            resp_thread = std::thread(&kv_storeImpl::serveRequest, this, std::ref(stop));
        }

    }

    kv_storeImpl::~kv_storeImpl() {
        if (resp_thread.joinable()) {
            resp_thread.join();
        }

        if (get_thread.joinable()) {
            get_thread.join();
        }
        
        if (put_thread.joinable()) {
            put_thread.join();
        }
        
        if (commit_thread.joinable()) {
            commit_thread.join();
        }
        
        if (ack_thread.joinable()) {
            ack_thread.join();
        }
        
    }

    grpc::Status kv_storeImpl::get(ServerContext* context, const getReq* request, reqStatus* response) {
        std::cout << "id: " << id <<  " GET CALLED!!" << std::endl;
        //// Sanity checks for get
        //if (request->key().length() > MAX_KEY_LEN) {
        //	response->set_value ("");
        //	response->set_status(KV_FAILURE);
        //	return Status::OK;
	    //}

        Request req = Request(*request);
        
        try {
            if (get_thread.joinable()) {
                get_thread.join();
            }
            get_thread = std::thread(&kv_storeImpl::get_process, this, req);
            response->set_status(KV_GET_SUCCESS);
        } catch (const std::exception& e) {
            std::cerr << "Error in get_process: " << e.what() << std::endl;
            return grpc::Status(grpc::StatusCode::INTERNAL, "Internal error");
        }
        return Status::OK;
    }

    void kv_storeImpl::get_process(Request req) {
        if (tail) {
            // TODO: push the request to pending queue
            std::cout << "pushing to pending Q" << std::endl;
            pending_q.enqueue(req); 
	        //std::cout << "Processing client get() request" << std::endl;
            //auto part_mgr = PartitionManager::get_instance();
            //auto partition = part_mgr->get_partition(request->key());
            //auto value = partition->get(request->key());
            //response->set_value(value);
            //if (value == "") {
            //    response->set_status(KV_GET_FAILED);
            //} else {
            //    response->set_status(KV_GET_SUCCESS);
            //}
            //response->set_value("get call successful");
            //response->set_status(KV_GET_SUCCESS);
            //return Status::OK;
        }
        else {
	        std::cout << "forwarding to tail" << std::endl;
            ClientContext _context;
            fwdGetReq _req = req.rpc_fwdGetReq();
            //auto *original_req = _req.mutable_req();
            //auto *meta = original_req->mutable_meta();
            //original_req->set_key(req.key);
            //meta->set_addr(req.addr);
            empty _resp;
            //TODO: response->set_status(KV_FWD_GET);
            Status status = tail_stub->fwdGet(&_context, _req, &_resp);
        }
    }

    grpc::Status kv_storeImpl::put(grpc::ServerContext* context, const putReq* request, reqStatus* response) {
        //TODO:
        // verify key and value
        //// std::cout << "PUT CALLED!!\n";
        //// Sanity checks for key and value
        //if (request->key().length() > MAX_KEY_LEN || request->value().length() > MAX_VALUE_LEN) {
        //	response->set_old_value("");
        //	response->set_status(KV_FAILURE);
        //	return grpc::Status::OK;
        //}
        //
        //// std::cout << "Processing client put() request\n";
        //auto part_mgr = PartitionManager::get_instance();
        //auto partition = part_mgr->get_partition(request->key());
        //auto old_value = partition->put(request->key(), request->value());
        //response->set_old_value(old_value);
        //if (old_value == "") {
        //    response->set_status(KV_PUT_SUCCESS);
        //} else {
        //    response->set_status(KV_UPDATE_SUCCESS);
        //}
	
	    std::cout << "id: " << id << " PUT CALLED!!\n";

        Request req = Request(*request);

        try {
            if (put_thread.joinable()) {
                put_thread.join();
            }
            put_thread = std::thread(&kv_storeImpl::put_process, this, req);
            response->set_status(KV_PUT_RECEIVED);
        } catch (const std::exception& e) {
            std::cerr << "Error in put_process: " << e.what() << std::endl;
            return grpc::Status(grpc::StatusCode::INTERNAL, "Internal error");
        }
        return Status::OK;
    }

    void kv_storeImpl::put_process(Request req) {
	    if (head) {
	        std::cout << "HEAD: " << head << ", received put() request\n";
            spawnCommit(req);
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
    
    grpc::Status kv_storeImpl::fwdGet(ServerContext* context, const fwdGetReq* request, empty* response) {
        std::cout << "id: " << id <<  " received fwdGetReq" << std::endl;
        assert(tail); // Only tail shoudl receive forwarded getReq
        std::cout << "pushing to pending Q" << std::endl;
        pending_q.enqueue(Request(*request));
        return Status::OK;
    }

    grpc::Status kv_storeImpl::fwdPut(ServerContext* context, const fwdPutReq* request, empty* response) {
	    std::cout << "id: " << id << " received fwdPutReq\n";
        assert(head);
	    Request req = Request(*request);
        spawnCommit(req);
	    return Status::OK;
    }

    grpc::Status kv_storeImpl::commit(ServerContext* context, const fwdPutReq* request, empty* response) {
	    std::cout << "id: " << id << " received commit\n";
        assert(!head);
        Request req = Request(*request);
        spawnCommit(req);
        return Status::OK;
    }
    
    grpc::Status kv_storeImpl::ack(ServerContext* context, const putAck* request, empty* response) {
	    std::cout << "id: " << id << " received ack\n";
        assert(!tail);
        Request req = Request(*request);
        spawnAck(req);
        return Status::OK;
    }

    void kv_storeImpl::spawnCommit(Request req) {
        try {
            if (commit_thread.joinable()) {
                commit_thread.join();
            }
            commit_thread = std::thread(&kv_storeImpl::commit_process, this, req);
        } catch (const std::exception& e) {
            std::cerr << "Error in spawnCommit: " << e.what() << std::endl;
        }
    }

    void kv_storeImpl::spawnAck(Request req) {
        try {
            if (ack_thread.joinable()) {
                ack_thread.join();
            }
            ack_thread = std::thread(&kv_storeImpl::ack_process, this, req);
        } catch (const std::exception& e) {
            std::cerr << "Error in spawnAck: " << e.what() << std::endl;
        }
    }

    void kv_storeImpl::commit_process(Request req) {
        // TODO:
        // 1. Commit to own database
        // Add to ThreadSafeHashMap
        local_map.insert(req.key, req.value);
        sent_q.enqueue(req);
        if (!tail) {
	        ClientContext _context;
            fwdPutReq _req = req.rpc_fwdPutReq();
            empty _resp;
            next_stub->commit(&_context, _req, &_resp);
        }
        else {
            std::cout << "tail receive commit from " << req.addr << std::endl;
            pending_q.enqueue(req);
        }
    }

    void kv_storeImpl::ack_process(Request req) {
        // Delete entry to sent_q
        Request curr_req = sent_q.dequeue();

        // Check if the request contains the same information as the ack. (Should never reach this code)
        if (!req.identicalRequests(curr_req)) {
            req.dumpRequestInfo();
            curr_req.dumpRequestInfo();
            assert(false);
        }

        if (!head) {
	        ClientContext _context;
            putAck _req = curr_req.rpc_putAck();
            empty _resp;
            prev_stub->ack(&_context, _req, &_resp); 
        }
        else {
            std::cout << "head received ack" << std::endl;
        }
    }

    void kv_storeImpl::serveRequest(std::atomic<bool> &stop) {
        Request req;
        while (!stop.load()) {
            std::cout << "waiting for pending_q" << std::endl;
            req = pending_q.dequeue();
            std::cout << "popped request from pending_q" << std::endl;
            if (req.type == request_t::GET) {
	            std::cout << "Processing client get() request" << std::endl;
                std::string client_addr = req.addr;
                std::cout << "client_addr: " << client_addr << std::endl;
                client_stub = KVResponse::NewStub(grpc::CreateChannel(client_addr, grpc::InsecureChannelCredentials()));
                ClientContext _context;

                // Call partition manager get to get the value
                getResp _req;
	            auto part_mgr = PartitionManager::get_instance();
                auto partition = part_mgr->get_partition(req.key);
                auto value = partition->get(req.key);
                _req.set_value(value);
                if (value == "") {
                    _req.set_status(KV_GET_FAILED);
                } else {
                    _req.set_status(KV_GET_SUCCESS);
                }
                respStatus _resp; 
                Status status = client_stub->sendGetResp(&_context, _req, &_resp);
                std::cout << "Sent get response to client" << std::endl;
            }
            else if (req.type == request_t::PUT) {
                // TODO
	            std::cout << "Processing client put() request" << std::endl;
	            std::string client_addr = req.addr;
	            std::cout << "client_addr: " << client_addr << std::endl;
                std::cout << "key: " << req.key << std::endl;
                std::cout << "value: " << req.value << std::endl;
	            client_stub = KVResponse::NewStub(grpc::CreateChannel(client_addr, grpc::InsecureChannelCredentials()));
	            ClientContext _context;

                // Call partition manager put to get the old value
	            putResp _req;
                auto part_mgr = PartitionManager::get_instance();
                auto partition = part_mgr->get_partition(req.key);
                auto old_value = partition->put(req.key, req.value);
                _req.set_old_value(old_value);
                if (old_value == "") {
                    _req.set_status(KV_PUT_SUCCESS);
                } else {
                    _req.set_status(KV_UPDATE_SUCCESS);
                }
	            respStatus _resp; 
	            Status status = client_stub->sendPutResp(&_context, _req, &_resp);
                spawnAck(req);
	            std::cout << "Sent put response to client" << std::endl;
            }
            else {
                std::cerr << "Invalid request type" << std::endl;
                std::exit(1);
            }
        }
        std::cout << "Finished work assigned to resp_thread" << std::endl;
    }

    //Node::Node(const std::string _addr, const std::string _prev_addr, const std::string _next_addr) : 
    //    addr(_addr),
    //    prev_addr(_prev_addr),
    //    next_addr(_next_addr)
    //{
    //    if (!prev_addr.empty()) {
    //        connectPrev();
    //    }
    //    if (!next_addr.empty()) {
    //        connectNext();
    //    }
    //}

    //Node::connectPrev() {
    //    prev_stub = NodeService::NewStub(grpc::CreateChannel(prev_addr, grpc::InsecureChannelCredentials()));
    //}

    //Node::connectNext() {
    //    next_stub = NodeService::NewStub(grpc::CreateChannel(next_addr, grpc::InsecureChannelCredentials()));
    //}


    //grpc::Status Node::get(grpc::ServerContext* context, const node::getReq* req, node::getResp* resp) {
    //    //TODO 
    //    std::cout << "ID: " << id << "get called" << std::endl;
    //
    //}
 
    //grpc::Status Node::put(grpc::ServerContext* context, const node::putReq* req, node::putResp* resp) {
    //    //TODO
    //    std::cout << "ID: " << id << "put called" << std::endl;
    //}
    //
    //grpc::Status Node::ack(grpc::ServerContext* context, const node::ackReq* req, node::ackResp* resp) {
    //    //TODO
    //    std::cout << "ID: " << id << "ack called" << std::endl;
    //}
    //   

    //Server::Server(uint32_t _id, const char* db_name, const std::string _addr, const std::string _prev_addr, const std::string _next_addr) :
    //    id(_id),
    //    service(db_name),
    //    node(_id, _addr, _prev_addr, _next_addr)
    //{} 

    //Server::Wait() {
    //    service

}
