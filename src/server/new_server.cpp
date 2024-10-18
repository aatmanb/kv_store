#include "new_server.h"
#include "data.h"
#include "partition_manager.h"

#define KV_FAILURE -1
#define KV_GET_SUCCESS 0
#define KV_GET_FAILED  1
#define KV_UPDATE_SUCCESS 2
#define KV_PUT_SUCCESS 3
#define KV_PUT_RECEIVED 4
#define KV_PUT_REDIRECT 5

#define MAX_KEY_LEN 128
#define MAX_VALUE_LEN 2048

using grpc::Channel;
using grpc::ClientContext;
using grpc::ServerContext;
using grpc::Status;

namespace key_value_store {
    static constexpr int CONNECTION_TIMEOUT = 100;

    // template<typename RequestType, typename ResponseType>
    // grpc::Status invoke_rpc_with_timeout(
    //         std::function<grpc::Status(grpc::ClientContext*, RequestType&, ResponseType*)> &func,
    //         grpc::ClientContext &ctx, RequestType &request, ResponseType &response) {
    //     auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(CONNECTION_TIMEOUT);
    //     ctx.set_Deadline(deadline);
    //     return func(&ctx, request, &response);
    // }

    void runServer(std::string &master_addr, std::string &local_addr) {
        kv_storeImpl2 service(master_addr, local_addr);
        
        grpc::EnableDefaultHealthCheckService(true);
        grpc::reflection::InitProtoReflectionServerBuilderPlugin();
        grpc::ServerBuilder builder;
        // Listen on the given address without any authentication mechanism.
        builder.AddListeningPort(local_addr, grpc::InsecureServerCredentials());
        // Register "service" as the instance through which we'll communicate with
        // clients. In this case it corresponds to an *synchronous* service.
        builder.RegisterService(&service);
        // Finally assemble the server.
        std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
        COUT << "Server listening on " << local_addr << std::endl;
        service.start();
        
        // Wait for the server to shutdown. Note that some other thread must be
        // responsible for shutting down the server for this call to ever return.
        server->Wait();
    }

    void kv_storeImpl2::start() {
        COUT << "Contacting master at: " << manager_addr << "\n";

        // Notify manager that this server has restarted
        grpc::ClientContext ctx;
        notifyRestartReq req;
        req.set_node(addr);
        notifyRestartResponse response;
        COUT << "Notifying manager about restart...\n";
        auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(CONNECTION_TIMEOUT);
        ctx.set_deadline(deadline);
        auto status = manager_stub->notifyRestart(&ctx, req, &response);
        COUT << "Manager has been notified\n";

        if (!status.ok()) {
            throw new std::runtime_error(status.error_message());
        }

        // db_name = response.db_path().c_str();
        prev_addr = response.pred_addr();
        head_addr = response.head_addr();

        if (!prev_addr.empty()) {
            prev_stub = kv_store::NewStub(grpc::CreateChannel(prev_addr, grpc::InsecureChannelCredentials()));
        }
        if (!head_addr.empty()) {
            head_stub = kv_store::NewStub(grpc::CreateChannel(head_addr, grpc::InsecureChannelCredentials()));
            is_head.store(false);
        } else {
            is_head.store(true);
        }

        ack_thread.start();
        commit_thread.start();
        get_thread.start();
        put_thread.start();
        resp_thread.start();
    }

    kv_storeImpl2::kv_storeImpl2(std::string &master_addr, std::string &addr):
            manager_addr(master_addr),
            addr(addr) {
        is_tail.store(true);

        if (!manager_addr.empty()) {
            manager_stub = master::NewStub(grpc::CreateChannel(manager_addr, grpc::InsecureChannelCredentials()));
        } else {
            // Manager address is empty
            throw new std::runtime_error("No manager address provided");
        }
        // Open connection to database
        std::string db_name = std::string("db_") + addr;
        db_utils = std::move(std::make_unique<DatabaseUtils>(db_name.c_str()));
        db_utils->open();
    }

    kv_storeImpl2::~kv_storeImpl2() {
        ack_thread.pause();
        commit_thread.pause();
        get_thread.pause();
        put_thread.pause();
        resp_thread.pause();
    }

    grpc::Status kv_storeImpl2::get(ServerContext* context, const getReq* request, reqStatus* response) {
        COUT << addr <<  " GET CALLED!!" << std::endl;
        Request req = Request(*request);
        try {
            get_thread.post(std::bind(&kv_storeImpl2::get_process, this, req));
            response->set_status(KV_GET_SUCCESS);
        } catch (const std::exception& e) {
            std::cerr << "Error in get_process: " << e.what() << std::endl;
            return grpc::Status(grpc::StatusCode::INTERNAL, "Internal error");
        }
        return Status::OK;
    }

    void kv_storeImpl2::get_process(Request req) {
        // Current node can serve the get request if the local database stores the most recent version        
        if (is_tail.load() || !uncommitted_updates_per_key[req.key]) {
            resp_thread.post(std::bind(&kv_storeImpl2::serveRequest, this, req));
        } else {
	        COUT << "forwarding to tail: " << tail_addr << std::endl;
            // Forward to tail
            ClientContext _context;
            fwdGetReq _req = req.rpc_fwdGetReq();
            empty _resp;
            auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(CONNECTION_TIMEOUT);
            _context.set_deadline(deadline);
            Status status = tail_stub->fwdGet(&_context, _req, &_resp);
        }
    }

    grpc::Status kv_storeImpl2::put(grpc::ServerContext* context, const putReq* request, reqStatus* response) {	
	    //COUT << addr << ": PUT CALLED!!\n";
        Request req = Request(*request);

        try {
            put_thread.post(std::bind(&kv_storeImpl2::put_process, this, req));
            response->set_status(KV_PUT_RECEIVED);
        } catch (const std::exception& e) {
            std::cerr << "Error in get_process: " << e.what() << std::endl;
            return grpc::Status(grpc::StatusCode::INTERNAL, "Internal error");
        }
        return Status::OK;
    }

    grpc::Status kv_storeImpl2::fail(grpc::ServerContext* context, const failCommand* request, empty* response) {
        COUT << addr << ": Fail called\n";
        bool clean = request->clean();
        if (clean) {
            grpc::ClientContext ctx;
            notifyFailureReq req;
            req.set_failednode(addr);
            empty response;
            auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(CONNECTION_TIMEOUT);
            ctx.set_deadline(deadline);
            manager_stub->notifyFailure(&ctx, req, &response);
            db_utils->close();
        }
        COUT << "exiting...\n";
        exit(-1);
        return grpc::Status::OK;
    }

    void kv_storeImpl2::put_process(Request req) {
	    if (is_head.load()) {
	        COUT << "HEAD: " << is_head.load() << ", received put() request\n";
            commit_thread.post(std::bind(&kv_storeImpl2::commit_process, this, req));
        }
        else {
            //COUT << "Fowarding put to head: " << head_addr << "\n";
            // TODO
	        // Acknowledge that we received the PUT request
	        // TODO: response->set_status(KV_PUT_RECEIVED);
	        // TODO: Store it in the db here
	        // Prepare the forwarding request
	        ClientContext _context;
            fwdPutReq _req = req.rpc_fwdPutReq();
	        //fwdPutReq _req;
	        //auto *original_req = _req.mutable_req();
	        //auto *meta = original_req->mutable_meta();
	        //original_req->set_key (req.key);
	        //original_req->set_value (req.value);
	        //meta->set_addr(req.addr);
	        empty _resp;
            auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(CONNECTION_TIMEOUT);
            _context.set_deadline(deadline);
	        Status status = head_stub->fwdPut(&_context, _req, &_resp);
        }
    }
    
    grpc::Status kv_storeImpl2::fwdGet(ServerContext* context, const fwdGetReq* request, empty* response) {
        //COUT << "id: " << id <<  " received fwdGetReq" << std::endl;
        // TODO(): This assertion could fail during reconfiguration
        assert(is_tail.load()); // Only tail shoudl receive forwarded getReq
        //COUT << "pushing to pending Q" << std::endl;
        resp_thread.post(std::bind(&kv_storeImpl2::serveRequest, this, Request(*request)));
        return Status::OK;
    }

    grpc::Status kv_storeImpl2::fwdPut(ServerContext* context, const fwdPutReq* request, empty* response) {
	    //COUT << addr << " received fwdPutReq\n";
        assert(is_head.load());
	    Request req = Request(*request);
        commit_thread.post(std::bind(&kv_storeImpl2::commit_process, this, req));
	    return Status::OK;
    }

    grpc::Status kv_storeImpl2::commit(ServerContext* context, const fwdPutReq* request, empty* response) {
	    //COUT << addr << " received commit\n";
        assert(!is_head.load());
        Request req = Request(*request);
        commit_thread.post(std::bind(&kv_storeImpl2::commit_process, this, req));
        return Status::OK;
    }
    
    grpc::Status kv_storeImpl2::ack(ServerContext* context, const putAck* request, empty* response) {
	    //COUT << addr << " received ack\n";
        assert(!is_tail.load());
        ack_thread.post(std::bind(&kv_storeImpl2::ack_process, this, Request(*request)));
        return Status::OK;
    }

    grpc::Status kv_storeImpl2::notifyPredFailure(grpc::ServerContext* context, 
                const notifyPredFailureReq* request, empty *response) {
        COUT << "Predecessor has failed. Reconfiguring...\n";
        bool was_head = request->washead();
        ack_thread.pause();
        
        if (was_head) {
            // Head has failed. Make current node the new head
            put_thread.pause();
            // Modify stubs
            prev_stub.reset();
            head_stub.reset();

            // Modify addresses
            prev_addr.clear();
            head_addr.clear();

            is_head.store(true);
            COUT << "Successfully changed head to current node\n";
            put_thread.start();
        } else {
            prev_addr = request->newpred();
            prev_stub = kv_store::NewStub(grpc::CreateChannel(prev_addr, grpc::InsecureChannelCredentials()));

            grpc::ClientContext ctx;
            notifySuccessorFailureReq req;
            req.set_newsuccessor(addr);
            req.set_wastail(false);
            if (!sent_queue.isEmpty()) {
                putReq last_put_req = sent_queue.front().value().rpc_putReq();
                req.set_allocated_lastputreq(&last_put_req);
            }
            empty resp;
            auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(CONNECTION_TIMEOUT);
            ctx.set_deadline(deadline);
            prev_stub->notifySuccessorFailure(&ctx, req, &resp);
        }
        COUT << "Reconfiguration done...\n";
        printConfig();
        ack_thread.start();
        return grpc::Status::OK;
    }

    grpc::Status kv_storeImpl2::notifySuccessorFailure(grpc::ServerContext* context, const notifySuccessorFailureReq* request, empty *response) {
        COUT << "Successor has failed. Reconfiguring...\n";
        std::string new_successor = request->newsuccessor();
        bool was_tail = request->wastail();
        assert(!was_tail);
        ack_thread.pause();
        commit_thread.pause();
        if (was_tail) {
            // /This is handled by notifyTailFailure
            // get_thread.pause();
            // COUT << "Processing tail failure at successor\n";

            // // Modify stubs
            // next_stub.reset();
            // tail_stub.reset();

            // // Modify addresses
            // next_addr.clear();
            // tail_addr.clear();

            // is_tail.store(true);
            // // Tail should hold connection to database
            // db_utils->open();
            // commit_sent_updates();
            // get_thread.start();
            // resp_thread.start();
        } else {
            // Modify address and stub for new successor
            next_addr = request->newsuccessor();
            next_stub.reset();
            next_stub = kv_store::NewStub(grpc::CreateChannel(next_addr, grpc::InsecureChannelCredentials()));
            auto last_put_req = request->lastputreq();
            // Notify new successor that its predecessor has failed
            // ClientContext ctx;
            // notifyPredFailureReq req;
            // req.set_newpred(addr);
            // req.set_washead(false);
            // empty resp;
            // next_stub->notifyPredFailure(&ctx, req, &resp);

            process_lost_updates(last_put_req);
        }
        COUT << "Reconfiguration done...\n";
        printConfig();
        commit_thread.start();
        ack_thread.start();
        return grpc::Status::OK;
    }

    grpc::Status kv_storeImpl2::addTailNode(grpc::ServerContext *context, const addTailNodeReq *req, 
                empty* response) {
        COUT << "Received request to add tail node\n";
        // Pause threads
        commit_thread.pause();
        put_thread.pause();
        get_thread.pause();
        resp_thread.pause();
        
        if (is_tail.load()) {
            // TODO(): Close connection to DB
            is_tail.store(false);
            next_stub.reset();

            next_addr = req->newtail();
            next_stub = kv_store::NewStub(grpc::CreateChannel(next_addr, grpc::InsecureChannelCredentials()));
        }

        tail_stub.reset();
        tail_addr = req->newtail();
        tail_stub = kv_store::NewStub(grpc::CreateChannel(tail_addr, grpc::InsecureChannelCredentials()));

        // Close connection to database so that new tail can open it (SQLite allows only one process to connect to a given db)
        // db_utils->close();
        // Sync db in current node with db in new node
        grpc::ClientContext ctx;
        empty empty_resp;
        auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(CONNECTION_TIMEOUT);
        ctx.set_deadline(deadline);
        std::unique_ptr<grpc::ClientWriter<dbEntry>> writer {tail_stub->syncDB(&ctx, &empty_resp)};
        db_utils->write_all_rows(writer);
        writer->WritesDone();
        writer->Finish();
        
        // Resume threads
        commit_thread.start();
        put_thread.start();
        get_thread.start();

        COUT << "Reconfiguration is successful. New tail is: " << tail_addr << "\n";

        return grpc::Status::OK;
    }

    grpc::Status kv_storeImpl2::notifyHeadFailure(grpc::ServerContext* context, const headFailureNotification* request, empty *response) {
        COUT << "Received notification about head failure\n";
        put_thread.pause();
        head_addr = request->new_head();
        head_stub.reset();
        head_stub = kv_store::NewStub(grpc::CreateChannel(head_addr, grpc::InsecureChannelCredentials()));
        put_thread.start();
        COUT << "Reconfiguration is successful\n";
        return grpc::Status::OK;
    }

    grpc::Status kv_storeImpl2::notifyTailFailure(grpc::ServerContext* context,
                const tailFailureNotification* request, empty *response) {
        ack_thread.pause();
        commit_thread.pause();
        COUT << addr << ": notifyTailFailure\n";
        get_thread.pause();
        tail_addr = request->new_tail();
        tail_stub.reset();
        if (addr == tail_addr) {
            COUT << "Processing tail failure at successor\n";

            // Modify stubs
            next_stub.reset();
            tail_stub.reset();

            // Modify addresses
            next_addr.clear();
            // tail_addr.clear();

            is_tail.store(true);
            // Tail should hold connection to database
            // db_utils->open();
            // commit_sent_updates();
            resp_thread.start();
        } else {
            tail_stub = kv_store::NewStub(grpc::CreateChannel(tail_addr, grpc::InsecureChannelCredentials()));
        }
        
        get_thread.start();
        commit_thread.start();
        ack_thread.start();
        COUT << "Reconfiguration is successful\n";
        return grpc::Status::OK;
    }

    // void kv_storeImpl2::commit_sent_updates() {
    //     while (true) {
    //         auto req_opt = sent_queue.front();
    //         if (!req_opt.has_value()) {
    //             break;
    //         }
    //         auto req = req_opt.value();
    //         auto old_value = db_utils->put_value(req.key.c_str(), req.value.c_str());
    //         if (!is_head.load()) {
    //             ack_thread.post(std::bind(&kv_storeImpl2::ack_process, this, req));
    //         }
    //     }
    // }

    void kv_storeImpl2::process_lost_updates(const putReq& last_req) {
        ThreadSafeQueue<Request> tmp_queue;
        std::cout << "here\n";
        bool found = false;
        while (true) {
            auto val = sent_queue.tryDequeue();
            if (!val.has_value()) {
                break;
            }
            
            const putReq req = val.value().rpc_putReq();
            if (found) {
                ClientContext context;
                empty _resp;
                auto fwd_put_req = Request(req).rpc_fwdPutReq();
                auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(CONNECTION_TIMEOUT);
                context.set_deadline(deadline);
                next_stub->commit(&context, fwd_put_req, &_resp);
            }
            if (Request(last_req).identicalRequests(req)) {
                found = true;
            }
            tmp_queue.enqueue(val.value());
        }
        sent_queue = std::move(tmp_queue);
    }

    void kv_storeImpl2::commit_process(Request req) {
        sent_queue.enqueue(req); // sent_queue should be updated after sending?
        if (!is_tail.load()) {
            /**
             * Break this into 2 steps:
             * - Thread 1: Write to database/hashmap, append to to_send queue
             * - Thread 2: Forward commit from to_send queue, append to sent queue
             */
            int dirty_updates = uncommitted_updates_per_key[req.key];
            uncommitted_updates_per_key.insert(req.key, dirty_updates+1);
            
            // Write into own db
            db_utils->put_value(req.key.c_str(), req.value.c_str());
	        ClientContext _context;
            fwdPutReq _req = req.rpc_fwdPutReq();
            empty _resp;
            auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(CONNECTION_TIMEOUT);
            _context.set_deadline(deadline);
            next_stub->commit(&_context, _req, &_resp);
        }
        else {
            resp_thread.post(std::bind(&kv_storeImpl2::serveRequest, this, req));
        }
    }

    void kv_storeImpl2::ack_process(Request req) {
        Request curr_req = sent_queue.dequeue();
        auto dirty_updates = uncommitted_updates_per_key[req.key];
        if (!is_tail.load() &&  dirty_updates) {
            if (dirty_updates == 1) {
                uncommitted_updates_per_key.erase(req.key);
            } else {
                uncommitted_updates_per_key.insert(req.key, dirty_updates-1);
            }
        }

        if (!req.identicalRequests(curr_req)) {
            req.dumpRequestInfo();
            curr_req.dumpRequestInfo();
            assert(false);
        }

        if (!is_head) {
	        ClientContext _context;
            putAck _req = curr_req.rpc_putAck();
            empty _resp;
            auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(CONNECTION_TIMEOUT);
            _context.set_deadline(deadline);
            prev_stub->ack(&_context, _req, &_resp); 
        }
        else {
            COUT << "head received ack" << std::endl;
        }
    }

    void kv_storeImpl2::serveRequest(Request &req) {
        std::string client_addr = req.addr;
        COUT << "client_addr: " << client_addr << std::endl;
        client_stub = KVResponse::NewStub(grpc::CreateChannel(client_addr, grpc::InsecureChannelCredentials()));
        ClientContext _context;
        respStatus _resp;

        if (req.type == request_t::GET) {
            COUT << "Processing client get() request" << std::endl;
            getResp _req;

            auto value = db_utils->get_value(req.key.c_str());
            COUT << "Value: " << value << "\n";
            _req.set_value(value);
            if (value == "") {
                _req.set_status(KV_GET_FAILED);
            } else {
                _req.set_status(KV_GET_SUCCESS);
            }
            
            // Send response to client
            auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(CONNECTION_TIMEOUT);
            _context.set_deadline(deadline);
            Status status = client_stub->sendGetResp(&_context, _req, &_resp);
        } else if (req.type == request_t::PUT) {
            //COUT << "Processing client put() request" << std::endl;
            putResp _req;
            auto old_value = db_utils->put_value(req.key.c_str(), req.value.c_str());

            _req.set_old_value(old_value);
            if (old_value == "") {
                _req.set_status(KV_PUT_SUCCESS);
            } else {
                _req.set_status(KV_UPDATE_SUCCESS);
            }

            // Send response to client
            auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::milliseconds(CONNECTION_TIMEOUT);
            _context.set_deadline(deadline);
            Status status = client_stub->sendPutResp(&_context, _req, &_resp);
            // Send ack to predecessor
            ack_thread.post(std::bind(&kv_storeImpl2::ack_process, this, req));
            //COUT << "Sent put response to client" << std::endl;
        } else {
            std::cerr << "Invalid request type" << std::endl;
            std::exit(1);
        }
    }

    grpc::Status kv_storeImpl2::heartBeat(grpc::ServerContext *context, 
            const empty* request, empty *response) {
        return grpc::Status::OK;
    }

    void kv_storeImpl2::printConfig() {
        COUT << "addr: " << addr << " head_addr: " << head_addr << " tail_addr: " << tail_addr << " prev_addr: " << prev_addr << " next_addr: " << next_addr << std::endl;
    }

    grpc::Status kv_storeImpl2::syncDB(grpc::ServerContext *context, grpc::ServerReader<dbEntry>* reader, empty *response) {
        dbEntry entry;
        COUT << "Starting db sync with current tail\n";
        auto start_time = std::chrono::system_clock::now();

        while (reader->Read(&entry)) {
            db_utils->put_value(entry.key().c_str(), entry.value().c_str());
        }

        auto end_time = std::chrono::system_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time-start_time);
        COUT << "DB sync took: " << duration.count() << "ms\n";
        return grpc::Status::OK;
    }
}
