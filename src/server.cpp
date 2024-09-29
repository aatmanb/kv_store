#include "server.h"
#include "data.h"
#include "partition_manager.h"

namespace key_value_store {
    grpc::Status kv_storeImpl::get(grpc::ServerContext* context, const getReq* request, getResp* response) {
        auto part_mgr = PartitionManager::get_instance();
        auto partition = part_mgr->get_partition(request->key());
        response->set_value(partition->get(request->key()));
        response->set_status(0);

        return grpc::Status::OK;
    }

    grpc::Status kv_storeImpl::put(grpc::ServerContext* context, const putReq* request, putResp* response) {
        auto part_mgr = PartitionManager::get_instance();
        auto partition = part_mgr->get_partition(request->key());
        partition->put(request->key(), request->value());
        return grpc::Status::OK;
    }

    void runServer(uint16_t port) {
        std::string server_address = absl::StrFormat("0.0.0.0:%d", port);
        kv_storeImpl service;
        
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