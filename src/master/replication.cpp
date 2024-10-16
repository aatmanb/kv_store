#include "replication.h"
#include "server/dbutils.h"
#include "thread"
#include <chrono>

namespace key_value_store {
    void ReplicationManager::add_node(const std::string &server, notifyRestartResponse *resp) {
        std::unique_lock<std::shared_mutex> lock {mtx};
        int volume = server_to_chain_map[server];
        auto& servers = active_servers[volume];
        if (servers.size()) {    
            // Notify the other nodes of node addition
            auto pred_addr = servers[servers.size() - 1];
            for (const auto& node: servers) {
                empty resp2;
                addTailNodeReq add_tail_req;
                add_tail_req.set_newtail(server);
                grpc::ClientContext ctx;
                node_to_conn_map[pred_addr]->addTailNode(&ctx, add_tail_req, &resp2);
            }

            resp->set_head_addr(servers[0]);
            resp->set_pred_addr(pred_addr);
        } else {
            resp->set_head_addr("");
            resp->set_pred_addr("");
        }
        
        node_to_conn_map[server] = std::move(kv_store::NewStub(grpc::CreateChannel(server, grpc::InsecureChannelCredentials())));
        servers.push_back(server);
            
        resp->set_db_path(get_db_name_for_volume(volume));
    }

    void ReplicationManager::remove_node(const std::string &server) {
        std::unique_lock<std::shared_mutex> lock {mtx};
        int volume = server_to_chain_map[server];
        auto& servers = active_servers[volume];
        int idx = std::find(servers.begin(), servers.end(), server) - servers.begin();
        if (!idx && servers.size() >= 2) {
            // Head failure
            auto new_head = servers[1];
            // Inform new head about the failure
            grpc::ClientContext ctx;
            notifyPredFailureReq req;
            req.set_washead(true);
            req.set_newpred("");
            empty empty_response;
            node_to_conn_map[new_head]->notifyPredFailure(&ctx, req, &empty_response);

            // Notify all other servers about the new head
            for (int i=2; i<servers.size(); i++) {
                headFailureNotification req1;
                req1.set_new_head(new_head);
                grpc::ClientContext ctx;
                node_to_conn_map[servers[i]]->notifyHeadFailure(&ctx, req1, &empty_response);
            }
        } else if (idx == servers.size() - 1 && servers.size() > 1) {
            // Tail failure
            auto new_tail = servers[idx-1];
            grpc::ClientContext ctx;
            notifySuccessorFailureReq req;
            req.set_wastail(true);
            req.set_newsuccessor("");
            empty empty_response;
            node_to_conn_map[servers[idx-1]]->notifySuccessorFailure(&ctx, req, &empty_response);

            // Notify all other servers about the new tail
            for (int i=0; i<idx-1; i++) {
                tailFailureNotification req1;
                req1.set_new_tail(new_tail);
                grpc::ClientContext ctx;
                node_to_conn_map[servers[i]]->notifyTailFailure(&ctx, req1, &empty_response);
            }
        } else {
            // Intermediate node failure
            auto new_tail = servers[idx-1];
            grpc::ClientContext ctx;
            notifySuccessorFailureReq req;
            req.set_wastail(true);
            req.set_newsuccessor("");
            empty empty_response;
            node_to_conn_map[servers[idx-1]]->notifySuccessorFailure(&ctx, req, &empty_response);
        }
        
        servers.erase(servers.begin() + idx);
        node_to_conn_map.erase(server);
    }

    void ReplicationManager::check_health() {
        while (run_health_check) {
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
                        std::cout << "Detected failure of " << elem.first << "\n";
                    }
                }
                std::this_thread::sleep_for(std::chrono::seconds(5));
            }

            for (auto& server: servers_to_remove) {
                remove_node(server);
            }
        }
    }

    void ReplicationManager::start_health_check() {
        health_check_thread = std::thread(&ReplicationManager::start_health_check, this);
    }

    ReplicationManager::~ReplicationManager() {
        run_health_check = false;
        std::cout << "Waiting for health checker to stop\n";
        if (health_check_thread.joinable()) {
            health_check_thread.join();
        }
        std::cout << "Health check service has stopped\n";
    }

}