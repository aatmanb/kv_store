#ifndef client_h__
#define client_h__

#include <iostream>
#include <memory>
#include <string>
#include <cstring>
#include <stdexcept>
#include <cstdlib>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include <grpcpp/grpcpp.h>
#include "kv_store.grpc.pb.h"

class client {
    public:
        client(std::shared_ptr<grpc::Channel> channel);

        int get(std::string key, std::string &value);
        int put(std::string key, std::string value, std::string &old_value);

        uint16_t id;

    private:
        std::unique_ptr<kv_store::Stub> stub_;
};

#endif  // client_h__
