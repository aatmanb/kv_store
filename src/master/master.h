#pragma once

#include "kv_store.grpc.pb.h"
#include "replication.h"

#include <grpcpp/grpcpp.h>

namespace key_value_store {
    void start_master_node(std::string &db_dir, std::string &config_path, int port);

    class MasterImpl : public master::Service {
    public:
        MasterImpl(std::string &db_dir);

        grpc::Status notifyRestart(grpc::ServerContext *context, const notifyRestartReq *req, notifyRestartResponse *resp) override;

        grpc::Status notifyFailure(grpc::ServerContext *context, const notifyFailureReq *req, empty *resp) override;
    private:
        ReplicationManager *inst;
    };

    
}
