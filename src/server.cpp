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


namespace key_value_store {
    grpc::Status kv_storeImpl::get(grpc::ServerContext* context, const getReq* request, getResp* response) {

        std::cout << "GET CALLED!!\n";
        // Sanity checks for get
        if (request->key().length() > MAX_KEY_LEN) {
        	response->set_value ("");
        	response->set_status(KV_FAILURE);
        	return grpc::Status::OK;
	    }
            
	    std::cout << "Processing client get() request\n";
        auto part_mgr = PartitionManager::get_instance();
        auto partition = part_mgr->get_partition(request->key());
        auto value = partition->get(request->key());
        response->set_value(value);
        if (value == "") {
            response->set_status(KV_GET_FAILED);
        } else {
            response->set_status(KV_GET_SUCCESS);
        }

        return grpc::Status::OK;
    }

    grpc::Status kv_storeImpl::put(grpc::ServerContext* context, const putReq* request, putResp* response) {
	    std::cout << "PUT CALLED!!\n";
        // Sanity checks for key and value
        if (request->key().length() > MAX_KEY_LEN || request->value().length() > MAX_VALUE_LEN) {
        	response->set_old_value("");
        	response->set_status(KV_FAILURE);
        	return grpc::Status::OK;
        }
        
        std::cout << "Processing client put() request\n";
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

    void runServer(uint16_t port, char *db_fname) {
        std::string server_address = absl::StrFormat("0.0.0.0:%d", port);
        kv_storeImpl service(db_fname);
        
        auto part_mgr = PartitionManager::get_instance();
        
        part_mgr->set_db_path_directory(db_fname);
        part_mgr->create_partitions();
        
        grpc::EnableDefaultHealthCheckService(true);
        grpc::reflection::InitProtoReflectionServerBuilderPlugin();
        grpc::ServerBuilder builder;
        // Listen on the given address without any authentication mechanism.
        builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
        // Register "service" as the instance through which we'll communicate with
        // clients. In this case it corresponds to an *synchronous* service.
        builder.RegisterService(&service);
        // Finally assemble the server.
        std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
        std::cout << "Server listening on " << server_address << std::endl;
        
        // Wait for the server to shutdown. Note that some other thread must be
        // responsible for shutting down the server for this call to ever return.
        server->Wait();
    }
}
