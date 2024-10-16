#include "master.h"
#include <grpcpp/ext/proto_server_reflection_plugin.h>

#define COUT std::cout << __FILE__ << ":" << __LINE__ << " " 

namespace key_value_store {
    grpc::Status MasterImpl::notifyRestart(grpc::ServerContext *context, const notifyRestartReq *req, 
            notifyRestartResponse *resp) {
        COUT << "Received restart notification from " << req->node() << "\n";
        inst->add_node(req->node(), resp);
        
        return grpc::Status::OK;
    }

    grpc::Status MasterImpl::notifyFailure(grpc::ServerContext *context, const notifyFailureReq *req, empty *resp) {
        inst->remove_node(req->failednode());
        return grpc::Status::OK;
    }

    MasterImpl::MasterImpl(std::string &db_dir) {
        inst = ReplicationManager::get_instance();
        inst->set_db_dir(db_dir);
    }

    void start_master_node(std::string &db_dir, std::string &config_path, int port) {
        std::string addr = "0.0.0.0:" + std::to_string(port);

        auto repl_inst = ReplicationManager::get_instance();
        repl_inst->configure_cluster(config_path);

        MasterImpl master {db_dir};
        grpc::EnableDefaultHealthCheckService(true);
        grpc::reflection::InitProtoReflectionServerBuilderPlugin();
        grpc::ServerBuilder builder;
        // Listen on the given address without any authentication mechanism.
        builder.AddListeningPort(addr, grpc::InsecureServerCredentials());

        // Register "service" as the instance through which we'll communicate with
        // clients. In this case it corresponds to an *synchronous* service.
        builder.RegisterService(&master);
        // Finally assemble the server.
        std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
        COUT << "Master server listening on port:" << port << std::endl;
        
        // Wait for the server to shutdown. Note that some other thread must be
        // responsible for shutting down the server for this call to ever return.
        server->Wait();
    }
}