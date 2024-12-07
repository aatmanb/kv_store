#include "master.h"
#include <grpcpp/ext/proto_server_reflection_plugin.h>

namespace key_value_store {
    grpc::Status MasterImpl::notifyRestart(grpc::ServerContext *context, const notifyRestartReq *req, notifyRestartResponse *resp) {
        SPDLOG_LOGGER_INFO(logger, "received restart notification from {}", req->node());
        COUT << "Received restart notification from " << req->node() << "\n";
        inst->add_node(req->node(), resp);
        
        return grpc::Status::OK;
    }

    grpc::Status MasterImpl::notifyFailure(grpc::ServerContext *context, const notifyFailureReq *req, empty *resp) {
        inst->remove_node(req->failednode(), req->leave());
        return grpc::Status::OK;
    }

    grpc::Status MasterImpl::getChainMetadata(grpc::ServerContext *context, const chainMetadataReq *req, 
            chainMetadataResponse *resp) {
        SPDLOG_LOGGER_DEBUG(logger, "received chain metadata request");
        auto metadata_res = inst->get_chain_metadata(req->partition());
        resp->set_alive(metadata_res.has_value());
        if (!metadata_res.has_value()) {
            return grpc::Status::OK;
        }
        auto metadata = metadata_res.value();
        resp->set_head_addr(metadata.first);
        resp->set_tail_addr(metadata.second);
        return grpc::Status::OK;
    }

    MasterImpl::MasterImpl(std::string &db_dir, std::string &config_path, std::string &log_dir) {
        std::string log_file_name = log_dir + "/spdlog_master" + ".log";
        COUT << log_file_name << std::endl;
        
        spdlog::flush_every(std::chrono::milliseconds(1));
        logger = spdlog::basic_logger_mt("basic_logger", log_file_name);
        // Set the logging level
        logger->set_level(spdlog::level::debug);
        logger->flush_on(spdlog::level::debug);

        //inst = ReplicationManager::get_instance();
        inst = new ReplicationManager();
        inst->configure(db_dir, config_path, logger);
    }

    void start_master_node(std::string &db_dir, std::string &config_path, int port, std::string &log_dir) {
        COUT << "starting master node\n";
        std::string addr = "0.0.0.0:" + std::to_string(port);

        MasterImpl master {db_dir, config_path, log_dir};
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
