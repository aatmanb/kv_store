#pragma once

#include <iostream>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_format.h"

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "build/kv_store.grpc.pb.h"

ABSL_FLAG(uint16_t, port, 50051, "Server port for the service");

namespace key_value_store {
    class kv_storeImpl final : public kv_store::Service {
        grpc::Status get(grpc::ServerContext* context, const getReq* request, getResp* response) override;

        grpc::Status put(grpc::ServerContext* context, const putReq* request, putResp* response) override;
    };

    void runServer(uint16_t port);
}