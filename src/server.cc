#include <iostream>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_format.h"

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "cmake/build/kv_store.grpc.pb.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;

ABSL_FLAG(uint16_t, port, 50051, "Server port for the service");

class kv_storeImpl final : public kv_store::Service {
	Status get(ServerContext* context, const getReq* request, getResp* response) override {
        std::cout << "servicing get() for client " << request->name() << std::endl;
		std::cout << "server finding value for key: " << request->key() << std::endl;
		// TODO: assign the correct status
		// 0 : key is present
		// 1 : key is not present
		// -1 : failure occured while querying
        std::string test = "This is a test string. If seen, get() call was successful";
        response->set_value(test);
		response->set_status(0);
		
        std::cout << "completed servicing get() for client " << request->name() << std::endl;
		return Status::OK;		
	}
	
	Status put(ServerContext* context, const putReq* request, putResp* response) override {
        std::cout << "servicing put() for client " << request->name() << std::endl;
        std::cout << "servicing client " << request->name() << std::endl;
		std::cout << "server adding key to database: " << request->key() << std::endl;
        std::string test = "This is a test string. If seen, put() call was successful";
		response->set_old_value(test);
        response->set_status(0);
        std::cout << "completed servicing put() for client " << request->name() << std::endl;
		return Status::OK;
	}
};

void runServer(uint16_t port) {
	std::string server_address = absl::StrFormat("0.0.0.0:%d", port);
	kv_storeImpl service;
	
	grpc::EnableDefaultHealthCheckService(true);
	grpc::reflection::InitProtoReflectionServerBuilderPlugin();
	ServerBuilder builder;
	// Listen on the given address without any authentication mechanism.
	builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
	// Register "service" as the instance through which we'll communicate with
	// clients. In this case it corresponds to an *synchronous* service.
	builder.RegisterService(&service);
	// Finally assemble the server.
	std::unique_ptr<Server> server(builder.BuildAndStart());
	std::cout << "Server listening on " << server_address << std::endl;
	
	// Wait for the server to shutdown. Note that some other thread must be
	// responsible for shutting down the server for this call to ever return.
	server->Wait();
}

int main(int argc, char** argv) {
  absl::ParseCommandLine(argc, argv);
  runServer(absl::GetFlag(FLAGS_port));
  return 0;
}
