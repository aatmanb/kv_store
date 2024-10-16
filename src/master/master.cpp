#include "master.h"
#include <grpcpp/ext/proto_server_reflection_plugin.h>

namespace key_value_store {
    grpc::Status MasterImpl::notifyRestart(grpc::ServerContext *context, const notifyRestartReq *req, 
            notifyRestartResponse *resp) {
        inst->add_node(req->node(), resp);
        
        return grpc::Status::OK;
    }

    grpc::Status MasterImpl::notifyFailure(grpc::ServerContext *context, const notifyFailureReq *req, empty *resp) {
        inst->remove_node(req->failednode());
        return grpc::Status::OK;
    }

    MasterImpl::MasterImpl() {
        inst = ReplicationManager::get_instance();
    }

    void start_master_node() {
        std::string addr = "0.0.0.0:0";
        int port;

        MasterImpl master {};
        grpc::EnableDefaultHealthCheckService(true);
        grpc::reflection::InitProtoReflectionServerBuilderPlugin();
        grpc::ServerBuilder builder;
        // Listen on the given address without any authentication mechanism.
        builder.AddListeningPort(addr, grpc::InsecureServerCredentials(), &port);
        // Register "service" as the instance through which we'll communicate with
        // clients. In this case it corresponds to an *synchronous* service.
        builder.RegisterService(&master);
        // Finally assemble the server.
        std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
        std::cout << "Server listening on port:" << port << std::endl;
        
        // Wait for the server to shutdown. Note that some other thread must be
        // responsible for shutting down the server for this call to ever return.
        server->Wait();
    }
}