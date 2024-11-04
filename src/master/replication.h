#pragma once

#include "server/singleton.h"
#include "kv_store.grpc.pb.h"
#include "utils/utils.h"

#include <vector>
#include <atomic>
#include <string>
#include <memory>
#include <thread>
#include <grpcpp/grpcpp.h>
#include <shared_mutex>

#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE
#include "spdlog/include/spdlog/spdlog.h"
#include "spdlog/include/spdlog/sinks/basic_file_sink.h"

namespace key_value_store {
    class ReplicationManager{ //}: public Singleton<ReplicationManager> {
    private:
        std::shared_mutex mtx;

        std::thread health_check_thread;

        std::atomic_bool run_health_check {true};

        //friend class Singleton<ReplicationManager>;

        std::unordered_map<std::string, int> server_to_chain_map;

        std::vector<std::vector<std::string>> active_servers;

        std::unordered_map<std::string, std::unique_ptr<kv_store::Stub>> node_to_conn_map;

        int num_volumes;

        static constexpr int health_check_interval = 500; // in milliseconds

        void check_health();

        std::string db_dir;
        
        std::shared_ptr<spdlog::logger> logger;
    
    public:
        ReplicationManager();

        virtual ~ReplicationManager();

        void configure_cluster(std::string &config_path);

        void add_node(const std::string &server, notifyRestartResponse* resp);

        void remove_node(const std::string &server, const bool leave=true);

        void start_health_check();

        // Return the chain metadata (addresses of head and tail servers)
        std::optional<std::pair<std::string, std::string>> get_chain_metadata(uint32_t i);

        void configure(std::string &db_dir, std::string &config_path, std::shared_ptr<spdlog::logger> logger);
        void print_chain(std::vector<std::string> &chain);
        void printGrpcStatus(grpc::Status status);
    };
}
