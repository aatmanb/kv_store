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

namespace key_value_store {
    class ReplicationManager: public Singleton<ReplicationManager> {
    private:
        std::shared_mutex mtx;

        std::thread health_check_thread;

        std::atomic_bool run_health_check {true};

        friend class Singleton<ReplicationManager>;

        std::unordered_map<std::string, int> server_to_chain_map;

        std::vector<std::vector<std::string>> active_servers;

        std::unordered_map<std::string, std::unique_ptr<kv_store::Stub>> node_to_conn_map;

        int num_volumes;

        static constexpr int health_check_interval = 500; // in ms

        void check_health();

        std::string db_dir;
    
    public:
        ReplicationManager() {}

        virtual ~ReplicationManager();

        void configure_cluster(std::string &config_path);

        void add_node(const std::string &server, notifyRestartResponse* resp);

        void remove_node(const std::string &server);

        void start_health_check();

        void set_db_dir(std::string &db_dir);
    };
}