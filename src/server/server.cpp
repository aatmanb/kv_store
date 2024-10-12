#include "server.h"
#include "data.h"
#include "partition_manager.h"

#define KV_FAILURE -1

#define KV_GET_SUCCESS 0
#define KV_GET_FAILED 1

#define KV_UPDATE_SUCCESS 0
#define KV_PUT_SUCCESS 1

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
    }

    grpc::Status kv_storeImpl::get(ServerContext* context, const getReq* request, getResp* response) {
        std::cout << "id: " << id <<  " GET CALLED!!" << std::endl;
        //// Sanity checks for get
        //if (request->key().length() > MAX_KEY_LEN) {
        //	response->set_value ("");
        //	response->set_status(KV_FAILURE);
        //	return Status::OK;
	    //}
        
        if (tail) {
	        std::cout << "Processing client get() request" << std::endl;
            //auto part_mgr = PartitionManager::get_instance();
            //auto partition = part_mgr->get_partition(request->key());
            //auto value = partition->get(request->key());
            //response->set_value(value);
            //if (value == "") {
            //    response->set_status(KV_GET_FAILED);
            //} else {
            //    response->set_status(KV_GET_SUCCESS);
            //}
            response->set_value("get call successful");
            response->set_status(KV_GET_SUCCESS);
            return Status::OK;
        }
        else {
	        std::cout << "forwarding to tail" << std::endl;
            ClientContext c_context;
            c_context.set_deadline(context->deadline());
            return tail_stub->get(&c_context, *request, response);
        }

    }

    grpc::Status kv_storeImpl::put(grpc::ServerContext* context, const putReq* request, putResp* response) {
        // std::cout << "PUT CALLED!!\n";
        // Sanity checks for key and value
        if (request->key().length() > MAX_KEY_LEN || request->value().length() > MAX_VALUE_LEN) {
        	response->set_old_value("");
        	response->set_status(KV_FAILURE);
        	return grpc::Status::OK;
        }
        
        // std::cout << "Processing client put() request\n";
        auto part_mgr = PartitionManager::get_instance();
        auto partition = part_mgr->get_partition(request->key());
        auto old_value = partition->put(request->key(), request->value());
        response->set_old_value(old_value);
        if (old_value == "") {
            response->set_status(KV_PUT_SUCCESS);
        } else {
            response->set_status(KV_UPDATE_SUCCESS);
        }
        return grpc::Status::OK;
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
