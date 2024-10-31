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
    // Master takes about 5 seconds to add a new node to the chain. Connection timeout should be greater than that.
    static constexpr int CONNECTION_TIMEOUT = 10; // seconds

    void runServer(int id, std::string &master_addr, std::string &local_addr, std::string &log_dir) {
        kv_storeImpl2 service(id, master_addr, local_addr, log_dir);
        
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
        SPDLOG_LOGGER_INFO(logger , "Contacting master at {}", manager_addr);
        COUT << "Contacting master at: " << manager_addr << "\n";

        // Notify manager that this server has restarted
        grpc::ClientContext ctx;
        notifyRestartReq req;
        req.set_node(addr);
        notifyRestartResponse response;
        SPDLOG_LOGGER_DEBUG(logger , "Notifiying manager about restart");
        COUT << "Notifying manager about restart...\n";
        auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::seconds(CONNECTION_TIMEOUT);
        ctx.set_deadline(deadline);
        auto status = manager_stub->notifyRestart(&ctx, req, &response);
        if (!status.ok()) {
            SPDLOG_LOGGER_CRITICAL(logger , "Restart failed");
            printGrpcStatus(status);
            throw new std::runtime_error(status.error_message());
        }

        SPDLOG_LOGGER_DEBUG(logger , "Manager has been notified");
        COUT << "Manager has been notified\n";
        db_name = response.db_path().c_str();
        db_utils = std::move(std::make_unique<DatabaseUtils>(db_name));
        db_utils->open();

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

    kv_storeImpl2::kv_storeImpl2(int _id, std::string &master_addr, std::string &addr, std::string &log_dir):
        id(_id),
        manager_addr(master_addr),
        addr(addr) {
        
        std::string log_file_name = log_dir + "spdlog_server_" + std::to_string(id) + ".log";
        COUT << log_file_name << std::endl;
        
        // Logging example
        spdlog::flush_every(std::chrono::milliseconds(1));
        //logger = spdlog::create<spdlog::sinks::basic_file_sink_mt>("basic_logger", log_file_name);
        logger = spdlog::basic_logger_mt("server_logger", log_file_name);
        // Set the logging level
        logger->set_level(spdlog::level::debug);
        logger->flush_on(spdlog::level::debug);
        SPDLOG_LOGGER_TRACE(logger , "Some trace message that will be evaluated.{} ,{}", 1, 3.23);
        SPDLOG_LOGGER_DEBUG(logger , "Some Debug message that will be evaluated.. {} ,{}", 1, 3.23);
        SPDLOG_LOGGER_INFO(logger , "Some Info message that will be evaluated.. {} ,{}", 1, 3.23);
        SPDLOG_LOGGER_WARN(logger , "Some Warn message that will be evaluated.. {} ,{}", 1, 3.23);
        SPDLOG_LOGGER_ERROR(logger , "Some Error message that will be evaluated.. {} ,{}", 1, 3.23);
        SPDLOG_LOGGER_CRITICAL(logger , "Some Critical message that will be evaluated.. {} ,{}", 1, 3.23);
        
        is_tail.store(true);

        if (!manager_addr.empty()) {
            SPDLOG_LOGGER_INFO(logger , "master addr is {}. Creating master stub...", manager_addr);
            manager_stub = master::NewStub(grpc::CreateChannel(manager_addr, grpc::InsecureChannelCredentials()));
        } else {
            // Manager address is empty
            throw new std::runtime_error("No manager address provided");
        }
        
    }

    kv_storeImpl2::~kv_storeImpl2() {
        ack_thread.pause();
        commit_thread.pause();
        get_thread.pause();
        put_thread.pause();
        resp_thread.pause();
    }

    grpc::Status kv_storeImpl2::get(ServerContext* context, const getReq* request, reqStatus* response) {
        //COUT << addr <<  " GET CALLED!!" << std::endl;
        Request req = Request(*request);
        try {
            SPDLOG_LOGGER_DEBUG(logger, "GET: Is it Retry?: {}", request->retry());
            get_thread.post(std::bind(&kv_storeImpl2::get_process, this, req));
            response->set_status(KV_GET_SUCCESS);
        } catch (const std::exception& e) {
            SPDLOG_LOGGER_CRITICAL(logger, "error in get_process: {}", e.what());
            std::cerr << "Error in get_process: " << e.what() << std::endl;
            return grpc::Status(grpc::StatusCode::INTERNAL, "Internal error");
        }
        return Status::OK;
    }

    void kv_storeImpl2::get_process(Request req) {
        SPDLOG_LOGGER_DEBUG(logger, "is_tail: {}, received get() request {}", is_tail.load(), req.dumpRequestInfo());
        if (is_tail.load()) {
            resp_thread.post(std::bind(&kv_storeImpl2::serveRequest, this, req));
        } else {
	        //COUT << "forwarding to tail: " << tail_addr << std::endl;
            ClientContext _context;
            fwdGetReq _req = req.rpc_fwdGetReq();
            empty _resp;
            auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::seconds(CONNECTION_TIMEOUT);
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
            SPDLOG_LOGGER_CRITICAL(logger, "error in put_process: {}", e.what());
            std::cerr << "Error in put_process: " << e.what() << std::endl;
            return grpc::Status(grpc::StatusCode::INTERNAL, "Internal error");
        }
        return Status::OK;
    }

    grpc::Status kv_storeImpl2::fail(grpc::ServerContext* context, const failCommand* request, empty* response) {
        SPDLOG_LOGGER_INFO(logger, "{}: fail called", addr);
        COUT << addr << ": Fail called\n";
        bool clean = request->clean();
        if (clean) {
            grpc::ClientContext ctx;
            notifyFailureReq req;
            req.set_failednode(addr);
            empty response;
            auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::seconds(CONNECTION_TIMEOUT);
            ctx.set_deadline(deadline);
            manager_stub->notifyFailure(&ctx, req, &response);
            db_utils->close();
        }
        exit(-1);
        return grpc::Status::OK;
    }

    void kv_storeImpl2::put_process(Request req) {
        SPDLOG_LOGGER_DEBUG(logger, "is_head: {}, received put() request {}", is_head.load(), req.dumpRequestInfo());
        COUT << "HEAD: " << is_head.load() << ", received put() request\n";
        if (is_head.load()) {
            // Check if it is a retry request
            if (req.retry) {
                // If it is in the queue already, return
                if(requestInQueue(req))
                    return;
            }
            commit_thread.post(std::bind(&kv_storeImpl2::commit_process, this, req));
        }
        else {
            //COUT << "Fowarding put to head: " << head_addr << "\n";
            // TODO
	        // Acknowledge that we received the PUT request
	        // TODO: response->set_status(KV_PUT_RECEIVED);
	        // TODO: Store it in the db here
	        // Prepare the forwarding request
            //SPDLOG_LOGGER_DEBUG (logger, "key: {}, value: {}, addr: {}", req.key, req.value, req.addr);
	        ClientContext _context;
            fwdPutReq _req = req.rpc_fwdPutReq();
	        //fwdPutReq _req;
	        //auto *original_req = _req.mutable_req();
	        //auto *meta = original_req->mutable_meta();
	        //original_req->set_key (req.key);
	        //original_req->set_value (req.value);
	        //meta->set_addr(req.addr);
	        empty _resp;
            auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::seconds(CONNECTION_TIMEOUT);
            _context.set_deadline(deadline);
            //SPDLOG_LOGGER_DEBUG (logger, "Retry value: {}", req.retry);
            //SPDLOG_LOGGER_DEBUG (logger, "Retry in _req: {}", _req.retry);
            SPDLOG_LOGGER_DEBUG (logger, "Put forwarded to HEAD");
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
        SPDLOG_LOGGER_DEBUG(logger, "received fwdPutReq");
	    //COUT << addr << " received fwdPutReq\n";
        assert(is_head.load());
	    Request req = Request(*request);
        //SPDLOG_LOGGER_DEBUG (logger, "Committing key: {}, value: {}, for client: {}", req.key, req.value, req.addr);
        //SPDLOG_LOGGER_DEBUG (logger, "Is it Retry?: {}", req.retry);           
        // Check if it is a retry request
        if (req.retry)
            if (requestInQueue(req))
               return Status::OK;
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
        ack_thread.post(std::bind(&kv_storeImpl2::ack_process, this, Request(*request), false));
        return Status::OK;
    }

    grpc::Status kv_storeImpl2::notifyPredFailure(grpc::ServerContext* context, const notifyPredFailureReq* request, empty *response) {
        SPDLOG_LOGGER_DEBUG(logger, "predecessor has failed. reconfiguring...");
        COUT << "Predecessor has failed. Reconfiguring...\n";
        bool was_head = request->washead();
        prev_addr = request->newpred();
        ack_thread.pause();
        
        if (was_head) {
            SPDLOG_LOGGER_DEBUG(logger, "predecessor was head. Changing head to current node");
            // Head has failed. Make current node the new head
            put_thread.pause();
            // Modify stubs
            prev_stub.reset();
            head_stub.reset();

            // Modify addresses
            prev_addr.clear();
            head_addr.clear();

            is_head.store(true);
            SPDLOG_LOGGER_DEBUG(logger, "successfully changed head to current node: {}", addr);
            COUT << "Successfully changed head to current node\n";
            put_thread.start();
        } else {
            SPDLOG_LOGGER_DEBUG(logger, "New predecessor is {}", prev_addr);
            prev_stub = kv_store::NewStub(grpc::CreateChannel(prev_addr, grpc::InsecureChannelCredentials()));

            grpc::ClientContext ctx;
            notifySuccessorFailureReq req;
            req.set_newsuccessor(addr);
            req.set_wastail(false);
            req.set_lastputreqvalid(false);
            if (!sent_queue.isEmpty()) {
                SPDLOG_LOGGER_DEBUG(logger, "sending last received put req to the new predecessor");
                printSentQState();
                SPDLOG_LOGGER_DEBUG(logger, "sent_queue.size(): {}", sent_queue.size());
                putReq last_put_req = sent_queue.front().value().rpc_putReq();
                req.set_allocated_lastputreq(&last_put_req);
                req.set_lastputreqvalid(true);
            }
            SPDLOG_LOGGER_TRACE(logger, "lastputreq: {}", Request(req.lastputreq()).dumpRequestInfo());
            empty resp;
            auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::seconds(CONNECTION_TIMEOUT);
            ctx.set_deadline(deadline);
            grpc::Status status = prev_stub->notifySuccessorFailure(&ctx, req, &resp);
            if (!status.ok()) {
                printGrpcStatus(status);
                std::exit(1);
            }
        }
        SPDLOG_LOGGER_DEBUG(logger, "reconfiguration done");
        COUT << "Reconfiguration done...\n";
        printConfig();
        ack_thread.start();
        return grpc::Status::OK;
    }

    grpc::Status kv_storeImpl2::notifySuccessorFailure(grpc::ServerContext* context, const notifySuccessorFailureReq* request, empty *response) {
        SPDLOG_LOGGER_DEBUG(logger, "successor has failed. reconfiguring...");
        COUT << "Successor has failed. Reconfiguring...\n";
        std::string new_successor = request->newsuccessor();
        bool was_tail = request->wastail();
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
            SPDLOG_LOGGER_DEBUG(logger,"new successor is {}", next_addr);
            next_stub.reset();
            next_stub = kv_store::NewStub(grpc::CreateChannel(next_addr, grpc::InsecureChannelCredentials()));
            auto last_put_req = request->lastputreq();
            process_lost_updates(last_put_req, !request->lastputreqvalid());
        }
        SPDLOG_LOGGER_DEBUG(logger, "reconfiguration done");
        COUT << "Reconfiguration done...\n";
        printConfig();
        commit_thread.start();
        ack_thread.start();
        return grpc::Status::OK;
    }

    grpc::Status kv_storeImpl2::addTailNode(grpc::ServerContext *context, const addTailNodeReq *req, empty* response) {
        SPDLOG_LOGGER_DEBUG(logger, "received request to add tail node");
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
        db_utils->close();
        
        // Resume threads
        commit_thread.start();
        put_thread.start();
        get_thread.start();

        SPDLOG_LOGGER_DEBUG(logger, "reconfiguration done. New tail is: {}", tail_addr);
        COUT << "Reconfiguration is successful. New tail is: " << tail_addr << "\n";

        return grpc::Status::OK;
    }

    grpc::Status kv_storeImpl2::notifyHeadFailure(grpc::ServerContext* context, const headFailureNotification* request, empty *response) {
        SPDLOG_LOGGER_DEBUG(logger, "received notification about head failure");
        COUT << "Received notification about head failure\n";
        put_thread.pause();
        head_addr = request->new_head();
        head_stub.reset();
        head_stub = kv_store::NewStub(grpc::CreateChannel(head_addr, grpc::InsecureChannelCredentials()));
        put_thread.start();
        SPDLOG_LOGGER_DEBUG(logger, "reconfiguration done");
        COUT << "Reconfiguration is successful\n";
        return grpc::Status::OK;
    }

    grpc::Status kv_storeImpl2::notifyTailFailure(grpc::ServerContext* context, const tailFailureNotification* request, empty *response) {
        SPDLOG_LOGGER_DEBUG(logger, "{}: notifyTailFailure", addr);
        COUT << addr << ": notifyTailFailure\n";
        ack_thread.pause();
        commit_thread.pause();
        get_thread.pause();
        tail_addr = request->new_tail();
        tail_stub.reset();
        if (addr == tail_addr) {
            SPDLOG_LOGGER_DEBUG(logger, "processing tail failure at predecessor");
            COUT << "Processing tail failure at predecessor\n";

            // Modify stubs
            next_stub.reset();
            tail_stub.reset();

            // Modify addresses
            next_addr.clear();
            // tail_addr.clear();

            is_tail.store(true);
            // Tail should hold connection to database
            db_utils->open();
            commit_sent_updates();
            resp_thread.start();
        } else {
            tail_stub = kv_store::NewStub(grpc::CreateChannel(tail_addr, grpc::InsecureChannelCredentials()));
        }
        get_thread.start();
        commit_thread.start();
        ack_thread.start();
        SPDLOG_LOGGER_DEBUG(logger, "reconfiguration is successful");
        COUT << "Reconfiguration is successful\n";
        return grpc::Status::OK;
    }

    void kv_storeImpl2::commit_sent_updates() {
        grpc::Status status;
        while (true) {
            auto req_opt = sent_queue.tryDequeue();
            if (!req_opt.has_value()) {
                break;
            }
            auto req = req_opt.value();
            status = servePutReq(req);
            if (!status.ok()) {
                SPDLOG_LOGGER_CRITICAL(logger, "Response from server to client should never fail");
                printGrpcStatus(status);
                // std::exit(1);
            }
            if (!is_head.load()) {
                // Skip dequeue since request has already been dequeued from the sent queue
               ack_thread.post(std::bind(&kv_storeImpl2::ack_process, this, req, true));
            }
        }
    }

    void kv_storeImpl2::process_lost_updates(const putReq& last_req, bool successor_queue_empty) {
        SPDLOG_LOGGER_DEBUG(logger, "sending lost updates to the new successor");
        COUT << "sending lost updates to the new successor\n";
        printSentQState();
        SPDLOG_LOGGER_DEBUG(logger, "sent_queue.size(): {}", sent_queue.size());
        SPDLOG_LOGGER_TRACE(logger, "successor_queue_empty: {}", successor_queue_empty);
        if (sent_queue.isEmpty()) {
            assert(successor_queue_empty);
            // TODO: assert that last_req is also empty/null because if this node's sent_queue is empty then it's successor's  sent_queue must also be empty
            return;
        }
        ThreadSafeQueue<Request> tmp_queue;
        bool found = false;
        Request _req = Request(last_req);
        SPDLOG_LOGGER_DEBUG(logger, "last request in the new successor {}", _req.dumpRequestInfo());

        // If new successor's sent queue is empty, do not try to find an identical request. Instead just send everything in your queue.
        if (!successor_queue_empty) {
            while (!found) {
                auto local_req_optional = sent_queue.tryDequeue();
                if (!local_req_optional.has_value()) {
                    SPDLOG_LOGGER_CRITICAL(logger, "We come here if there is no request in sent queue which is identical to last_req. This is not possible");
                    std::exit(1);
                }
                auto local_req = local_req_optional.value();
                SPDLOG_LOGGER_DEBUG(logger, "local_req: {}", local_req.dumpRequestInfo());

                tmp_queue.enqueue(local_req);
                //const putReq req = val.value().rpc_putReq();
                if (_req.identicalRequests(local_req)) {
                    // Found a match. Send all the requests afer this request
                    SPDLOG_LOGGER_DEBUG(logger, "Found identical request");
                    found = true;
                }
            }
        }

        while(true) {
            SPDLOG_LOGGER_DEBUG(logger, "Sending all the remaining requests in sent_queue to the new successor");
            auto local_req = sent_queue.tryDequeue();
            if (!local_req.has_value()) {
                SPDLOG_LOGGER_DEBUG(logger, "All the remaining requests in sent_queue have been sent to the new successor");
                sent_queue = std::move(tmp_queue);
                return;
            }
            
            tmp_queue.enqueue(local_req.value());
            
            ClientContext context;
            empty _resp;
            auto fwd_put_req = local_req.value().rpc_fwdPutReq();
            auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::seconds(CONNECTION_TIMEOUT);
            context.set_deadline(deadline);
            next_stub->commit(&context, fwd_put_req, &_resp);
        }

        SPDLOG_LOGGER_CRITICAL(logger, "We should never return from here. return should be from inside the while loop");
        std::exit(1);
        return;
    }

    void kv_storeImpl2::commit_process(Request req) {
        SPDLOG_LOGGER_DEBUG(logger, "is_tail: {},  received commit from predecessor {} for request {}", is_tail.load(), prev_addr, req.dumpRequestInfo());
        // TODO:
        // 1. Commit to own database
        local_map.insert(req.key, req.value);
        sent_queue.enqueue(req);
        SPDLOG_LOGGER_DEBUG(logger, "sent_queue.size(): {}", sent_queue.size());
        if (!is_tail.load()) {
            /**
             * Break this into 2 steps:
             * - Thread 1: Write to database/hashmap, append to to_send queue
             * - Thread 2: Forward commit from to_send queue, append to sent queue
             */
	        ClientContext _context;
            fwdPutReq _req = req.rpc_fwdPutReq();
            empty _resp;
            auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::seconds(CONNECTION_TIMEOUT);
            _context.set_deadline(deadline);
            next_stub->commit(&_context, _req, &_resp);
        }
        else {
            resp_thread.post(std::bind(&kv_storeImpl2::serveRequest, this, req));
        }
    }

    void kv_storeImpl2::ack_process(Request req, bool skip_dequeue) {
        if (!skip_dequeue) {
            auto curr_req_optional = sent_queue.tryDequeue();
            if (!curr_req_optional.has_value()) {
                SPDLOG_LOGGER_CRITICAL(logger, "Cannot receive ack for a request not in sent queue");
                std::exit(1);
            }
            auto curr_req = curr_req_optional.value();
            SPDLOG_LOGGER_DEBUG(logger, "is_head: {},  received ack from successor {} for request {}", is_head.load(), next_addr, curr_req.dumpRequestInfo());
            SPDLOG_LOGGER_TRACE(logger, "sent_queue.size(): {}", sent_queue.size());

            if (!req.identicalRequests(curr_req)) {
                SPDLOG_LOGGER_DEBUG(logger, req.dumpRequestInfo().c_str());
                SPDLOG_LOGGER_DEBUG(logger, curr_req.dumpRequestInfo().c_str());
                SPDLOG_LOGGER_CRITICAL(logger, "Cannot receive ack for a request not in sent queue");
                assert(false);
            }
        }

        if (!is_head.load()) {
	        ClientContext _context;
            putAck _req = req.rpc_putAck();
            empty _resp;
            auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::seconds(CONNECTION_TIMEOUT);
            _context.set_deadline(deadline);
            prev_stub->ack(&_context, _req, &_resp); 
        }
    }

    void kv_storeImpl2::serveRequest(Request &req) {
        assert(is_tail.load());
        grpc::Status status;

        if (req.type == request_t::GET) {
            status = serveGetReq(req);
        } else if (req.type == request_t::PUT) {
            status = servePutReq(req);
        } else {
            std::cerr << "Invalid request type" << std::endl;
            std::exit(1);
        }

        if (!status.ok()) {
            SPDLOG_LOGGER_CRITICAL(logger, "response from server to client should never fail");
            printGrpcStatus(status);
            // std::exit(1);
        }

        if (req.type == request_t::PUT) { 
            // Send ack to predecessor
            ack_thread.post(std::bind(&kv_storeImpl2::ack_process, this, req, false));
        }
    }

    grpc::Status kv_storeImpl2::serveGetReq(Request &req) {
        assert(req.type == request_t::GET);
        SPDLOG_LOGGER_DEBUG (logger, "Processing client get() request");
        std::unique_ptr<KVResponse::Stub> client_stub = createClientStub(req.addr);
        ClientContext _context;
        respStatus _resp;

        getResp _req;

        auto value = db_utils->get_value(req.key.c_str());
        _req.set_value(value);
        if (value == "") {
            _req.set_status(KV_GET_FAILED);
        } else {
            _req.set_status(KV_GET_SUCCESS);
        }
        
        // Send response to client
        auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::seconds(CONNECTION_TIMEOUT);
        _context.set_deadline(deadline);
        Status status = client_stub->sendGetResp(&_context, _req, &_resp);
        return status;
    }

    grpc::Status kv_storeImpl2::servePutReq(Request &req) {
        assert(req.type == request_t::PUT);
        SPDLOG_LOGGER_DEBUG (logger, "Processing client put() request");
        std::unique_ptr<KVResponse::Stub> client_stub = createClientStub(req.addr);
        ClientContext _context;
        respStatus _resp;
        
        putResp _req;
        auto old_value = db_utils->put_value(req.key.c_str(), req.value.c_str());
        //SPDLOG_LOGGER_DEBUG (logger, "Processing client put() request");
        //SPDLOG_LOGGER_DEBUG (logger, "PUT: Client: {}, Key: {}, Old_value: {}, New Value: {}", req.addr, req.key, old_value, req.value);

        _req.set_old_value(old_value);
        if (old_value == "") {
            _req.set_status(KV_PUT_SUCCESS);
        } else {
            _req.set_status(KV_UPDATE_SUCCESS);
        }

        // Send response to client
        auto deadline = std::chrono::high_resolution_clock::now() + std::chrono::seconds(CONNECTION_TIMEOUT);
        _context.set_deadline(deadline);
        Status status = client_stub->sendPutResp(&_context, _req, &_resp);
        return status;
    }

    std::unique_ptr<KVResponse::Stub> kv_storeImpl2::createClientStub(std::string addr) {
            return std::move(KVResponse::NewStub(grpc::CreateChannel(addr, grpc::InsecureChannelCredentials())));
    }

    grpc::Status kv_storeImpl2::heartBeat(grpc::ServerContext *context, const empty* request, empty *response) {
        SPDLOG_LOGGER_DEBUG(logger, "sending heartbeat");
        return grpc::Status::OK;
    }

    void kv_storeImpl2::printConfig() {
        SPDLOG_LOGGER_DEBUG(logger, "addr: {}, head_addr: {}, tail_addr: {}, prev_addr: {}, next_addr: {}", addr, head_addr, tail_addr, prev_addr, next_addr);
        COUT << "addr: " << addr << " head_addr: " << head_addr << " tail_addr: " << tail_addr << " prev_addr: " << prev_addr << " next_addr: " << next_addr << std::endl;
    }
    
    void kv_storeImpl2::printGrpcStatus(grpc::Status status) {
        SPDLOG_LOGGER_CRITICAL(logger, "gRPC called failed");
        std::cout << "Code: " << status.error_code() << "\nError message: " << status.error_message() << "\nError details: " << status.error_details() << std::endl;
    }

    bool kv_storeImpl2::requestInQueue(Request req) {
        /**
         * Pausing the ack and commit threads to avoid concurrent updates on the sent queue
         * TODO(): This is a stop-gap solution. It would cause issues when the head node is processing failures. 
         * Ideally we should use a lock on the sent queue
         */
        ack_thread.pause();
        commit_thread.pause();

        SPDLOG_LOGGER_DEBUG (logger, "Checking for duplicate request");
        ThreadSafeQueue<Request> tmp_queue;
        bool found = false;
        while (!sent_queue.isEmpty()) {
            Request curr_req = sent_queue.dequeue();
            if (req.identicalRequests(curr_req) && req.type == request_t::PUT) {
                found = true;
                SPDLOG_LOGGER_DEBUG (logger, "Current request is duplicate. Req. addr: {}, Req. key: {}, Req. value: {}", req.addr, req.key, req.value);
            }
            SPDLOG_LOGGER_DEBUG (logger, "Found?: {}", found);
            tmp_queue.enqueue(curr_req);
        }
        sent_queue = std::move(tmp_queue);

        commit_thread.start();
        ack_thread.start();

        return found;
    }
        
    void kv_storeImpl2::printSentQState() {
        ThreadSafeQueue<Request> tmp_queue;
        int idx = 0;
        while (!sent_queue.isEmpty()) {
            auto _req = sent_queue.tryDequeue().value();
            tmp_queue.enqueue(_req);
            SPDLOG_LOGGER_DEBUG (logger, "idx[{}]: {}", idx, _req.dumpRequestInfo());
            idx++;
        }
        sent_queue = std::move(tmp_queue);
    }
}
