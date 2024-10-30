#include "replication.h"
#include "server/dbutils.h"
#include "thread"

#include <chrono>

namespace key_value_store {
    void ReplicationManager::print_chain(std::vector<std::string> &chain) {
        if (!chain.size()) return;
        int i;
        SPDLOG_LOGGER_DEBUG(logger, "(Head) {}", chain[0]);
        for (i=1; i<chain.size()-1; i++) {
            SPDLOG_LOGGER_DEBUG(logger, "-> {}", chain[i]);
        }

        if (i == chain.size() - 1) { // there are atleast two servers
            SPDLOG_LOGGER_DEBUG(logger, "-> {} (Tail)", chain[i]);
        }
        else {
            SPDLOG_LOGGER_DEBUG(logger, " (Tail)");
        }
    }

    ReplicationManager::ReplicationManager() {
            start_health_check();
    }

    void ReplicationManager::add_node(const std::string &server, notifyRestartResponse *resp) {
        std::unique_lock<std::shared_mutex> lock {mtx};

        int volume = server_to_chain_map[server];
        SPDLOG_LOGGER_DEBUG(logger, "Adding {} to volume {}", server, volume);
        auto& servers = active_servers[volume];
        if (servers.size()) {    
            // Notify the other nodes of node addition
            for (const auto& node: servers) {
                empty resp2;
                addTailNodeReq add_tail_req;
                add_tail_req.set_newtail(server);
                grpc::ClientContext ctx;
                node_to_conn_map[node]->addTailNode(&ctx, add_tail_req, &resp2);
            }

            auto pred_addr = servers[servers.size() - 1];
            resp->set_head_addr(servers[0]);
            resp->set_pred_addr(pred_addr);
        } else {
            resp->set_head_addr("");
            resp->set_pred_addr("");
        }
        
        COUT << "Successfully added " << server << " to the chain\n";
        SPDLOG_LOGGER_DEBUG(logger, "successfully added {} to the chain", server);
        node_to_conn_map[server] = std::move(kv_store::NewStub(grpc::CreateChannel(server, grpc::InsecureChannelCredentials())));
        active_servers[volume].push_back(server);
            
        resp->set_db_path(db_dir + get_db_name_for_volume(volume));
        print_chain(active_servers[volume]);
    }

    void ReplicationManager::remove_node(const std::string &server) {
        SPDLOG_LOGGER_DEBUG(logger, "handling failure of server {}", server);
        COUT << "Handling failure of server: " << server << "\n";
        std::unique_lock<std::shared_mutex> lock {mtx};
        int volume = server_to_chain_map[server];
        auto& servers = active_servers[volume];
        int idx = std::find(servers.begin(), servers.end(), server) - servers.begin();
        if (!idx && servers.size() > 1) {
            SPDLOG_LOGGER_DEBUG(logger, "processing head failure");
            COUT << "Processing head failure...\n";
            // Head failure
            auto new_head = servers[1];
            // Inform new head about the failure
            grpc::ClientContext ctx;
            notifyPredFailureReq req;
            req.set_washead(true);
            req.set_newpred("");
            empty empty_response;
            SPDLOG_LOGGER_DEBUG(logger, "contacting new head");
            COUT << "Contacting new head...\n";
            node_to_conn_map[new_head]->notifyPredFailure(&ctx, req, &empty_response);

            // Notify all other servers about the new head
            for (int i=2; i<servers.size(); i++) {
                SPDLOG_LOGGER_DEBUG(logger, "sending notifyHeadFailure to server {}", servers[i]);
                headFailureNotification req1;
                req1.set_new_head(new_head);
                grpc::ClientContext ctx;
                node_to_conn_map[servers[i]]->notifyHeadFailure(&ctx, req1, &empty_response);
            }
        } else if (idx == servers.size() - 1 && servers.size() > 1) {
            SPDLOG_LOGGER_DEBUG(logger, "processing tail failure");
            COUT << "Processing tail failure\n";
            // Tail failure
            auto new_tail = servers[idx-1];
            // grpc::ClientContext ctx;
            // notifySuccessorFailureReq req;
            // req.set_wastail(true);
            // req.set_newsuccessor("");
            // empty empty_response;
            // node_to_conn_map[servers[idx-1]]->notifySuccessorFailure(&ctx, req, &empty_response);

            // Notify all other servers about the new tail
            for (int i=idx-1; i>=0; i--) {
                SPDLOG_LOGGER_DEBUG(logger, "sending notifyTailFailure to server {}", servers[i]);
                tailFailureNotification req1;
                req1.set_new_tail(new_tail);
                grpc::ClientContext ctx;
                empty empty_response;
                node_to_conn_map[servers[i]]->notifyTailFailure(&ctx, req1, &empty_response);
            }
        } else if (idx && (idx + 1) < servers.size()) {
            SPDLOG_LOGGER_DEBUG(logger, "processing intermediate node failure");
            COUT << "Processing intermediate node failure\n";
            // Intermediate node failure
            notifyPredFailureReq req;
            req.set_newpred(servers[idx-1]);
            req.set_washead(false);
            grpc::ClientContext ctx;
            empty empty_response;
            SPDLOG_LOGGER_DEBUG(logger, "sending notifyPredFailure to server {}", servers[idx+1]);
            node_to_conn_map[servers[idx+1]]->notifyPredFailure(&ctx, req, &empty_response);
        }
        
        servers.erase(servers.begin() + idx);
        node_to_conn_map.erase(server);
        print_chain(active_servers[volume]);
        SPDLOG_LOGGER_INFO(logger, "reconfiguration done");
        COUT << "Reconfiguration done\n";
    }

    void ReplicationManager::check_health() {
        SPDLOG_LOGGER_INFO(logger, "launched thread for health checking of servers");
        COUT << "Launched thread for checking health of servers\n";
        while (run_health_check) {
            SPDLOG_LOGGER_DEBUG(logger, "sending heartbeats");
            COUT << "Sending heartbeats\n";
            std::vector<std::string> servers_to_remove;
            {
                std::shared_lock<std::shared_mutex> lock {mtx};
                for (auto& elem: node_to_conn_map) {
                    // TODO(): Make this async
                    grpc::ClientContext context;
                    empty req;
                    empty resp;
                    auto status = elem.second->heartBeat(&context, req, &resp);

                    if (!status.ok()) {
                        servers_to_remove.push_back(elem.first);
                        SPDLOG_LOGGER_INFO(logger, "detected failure of node {}", elem.first);
                        printGrpcStatus(status);
                        COUT << "Detected failure of node: " << elem.first << "\n";
                    }
                }
            }

            for (auto& server: servers_to_remove) {
                remove_node(server);
            }
            std::this_thread::sleep_for(std::chrono::seconds(health_check_interval));
        }
    }

    void ReplicationManager::start_health_check() {
        health_check_thread = std::thread(&ReplicationManager::check_health, this);
    }

    ReplicationManager::~ReplicationManager() {
        run_health_check = false;
        if (health_check_thread.joinable()) {
            SPDLOG_LOGGER_DEBUG(logger, "waiting for health checker to stop");
            COUT << "Waiting for health checker to stop\n";
            health_check_thread.join();
        }
        SPDLOG_LOGGER_INFO(logger, "health checker has stopped");
        COUT << "Health check service has stopped\n";
    }

    void ReplicationManager::configure(std::string &db_dir, std::string &config_path, std::shared_ptr<spdlog::logger> logger) {
        this->db_dir = db_dir;
        this->logger = logger;

        configure_cluster(config_path);
    }

    void ReplicationManager::configure_cluster(std::string &config_path) {
        COUT << "configuring cluster\n";
        SPDLOG_LOGGER_INFO(logger, "configuring cluster");
        auto partitions = parseConfigFile(config_path);
        num_volumes = partitions.size();
        active_servers.resize(partitions.size());
        int i = 0;
        for (const auto& part: partitions) {
            for (const auto& server: part.get_servers()) {
                // Server contains only the port. Get the actual address
                auto server_with_hostname = "0.0.0.0:" + server;
                server_to_chain_map[server_with_hostname] = i;
                server_with_hostname = "localhost:" + server;
                server_to_chain_map[server_with_hostname] = i;
            }
            i++;
        }
        SPDLOG_LOGGER_INFO(logger, "configuration done");
    }

    void ReplicationManager::printGrpcStatus(grpc::Status status) {
        SPDLOG_LOGGER_CRITICAL(logger, "gRPC called failed.\nError message: {}\nError details: {}", status.error_message(), status.error_details());
    }

}
