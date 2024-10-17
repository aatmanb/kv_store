#include "replication.h"
#include "server/dbutils.h"
#include "thread"

#include <chrono>

namespace key_value_store {
    void print_chain(std::vector<std::string> &chain) {
        if (!chain.size()) return;
        COUT << "(Head) " << chain[0];
        for (int i=1; i<chain.size(); i++) {
            std::cout << " -> " << chain[i];
        }
        std::cout << "  (Tail)\n";        
    }

    void ReplicationManager::add_node(const std::string &server, notifyRestartResponse *resp) {
        std::unique_lock<std::shared_mutex> lock {mtx};

        int volume = server_to_chain_map[server];
        COUT << "Adding " << server << " to volume: " << volume << "\n";
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
        node_to_conn_map[server] = std::move(kv_store::NewStub(grpc::CreateChannel(server, grpc::InsecureChannelCredentials())));
        active_servers[volume].push_back(server);
            
        resp->set_db_path(db_dir + get_db_name_for_volume(volume));
        print_chain(active_servers[volume]);
    }

    void ReplicationManager::remove_node(const std::string &server) {
        COUT << "Handling failure of server: " << server << "\n";
        std::unique_lock<std::shared_mutex> lock {mtx};
        int volume = server_to_chain_map[server];
        auto& servers = active_servers[volume];
        int idx = std::find(servers.begin(), servers.end(), server) - servers.begin();
        if (!idx && servers.size() > 1) {
            COUT << "Processing head failure...\n";
            // Head failure
            auto new_head = servers[1];
            // Inform new head about the failure
            grpc::ClientContext ctx;
            notifyPredFailureReq req;
            req.set_washead(true);
            req.set_newpred("");
            empty empty_response;
            COUT << "Contacting new head...\n";
            node_to_conn_map[new_head]->notifyPredFailure(&ctx, req, &empty_response);

            // Notify all other servers about the new head
            for (int i=2; i<servers.size(); i++) {
                headFailureNotification req1;
                req1.set_new_head(new_head);
                grpc::ClientContext ctx;
                node_to_conn_map[servers[i]]->notifyHeadFailure(&ctx, req1, &empty_response);
            }
        } else if (idx == servers.size() - 1 && servers.size() > 1) {
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
                tailFailureNotification req1;
                req1.set_new_tail(new_tail);
                grpc::ClientContext ctx;
                empty empty_response;
                node_to_conn_map[servers[i]]->notifyTailFailure(&ctx, req1, &empty_response);
            }
        } else if (idx && (idx + 1) < servers.size()) {
            COUT << "Processing intermediate node failure\n";
            // Intermediate node failure
            notifyPredFailureReq req;
            req.set_newpred(servers[idx-1]);
            req.set_washead(false);
            grpc::ClientContext ctx;
            empty empty_response;
            node_to_conn_map[servers[idx+1]]->notifyPredFailure(&ctx, req, &empty_response);
        }
        
        servers.erase(servers.begin() + idx);
        node_to_conn_map.erase(server);
        print_chain(active_servers[volume]);
        COUT << "Reconfiguration done\n";
    }

    void ReplicationManager::check_health() {
        COUT << "Launched thread for checking health of servers\n";
        while (run_health_check) {
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
                        COUT << "Detected failure of node: " << elem.first << "\n";
                    }
                }
                std::this_thread::sleep_for(std::chrono::seconds(health_check_interval));
            }

            for (auto& server: servers_to_remove) {
                remove_node(server);
            }
        }
    }

    void ReplicationManager::start_health_check() {
        health_check_thread = std::thread(&ReplicationManager::check_health, this);
    }

    ReplicationManager::~ReplicationManager() {
        run_health_check = false;
        COUT << "Waiting for health checker to stop\n";
        if (health_check_thread.joinable()) {
            health_check_thread.join();
        }
        COUT << "Health check service has stopped\n";
    }

    void ReplicationManager::set_db_dir(std::string &db_dir) {
        this->db_dir = db_dir;
    }

    void ReplicationManager::configure_cluster(std::string &config_path) {
        auto partitions = parseConfigFile(config_path);
        num_volumes = partitions.size();
        active_servers.resize(partitions.size());
        int i = 0;
        for (const auto& part: partitions) {
            for (const auto& server: part.get_servers()) {
                server_to_chain_map[server] = i;
            }
            i++;
        }
    }

}
