#include <iostream>
#include <string>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_format.h"

#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/health_check_service_interface.h>

#include "cmake/build/kv_store.grpc.pb.h"
#include "dbutils.h"

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::Status;
using namespace key_value_store;

#define KV_KEY_ERROR -1
#define KV_GET_FAILED 0
#define KV_GET_SUCCESS 1

#define MAX_KEY_LEN 128
#define MAX_VALUE_LEN 2048

#define KV_PUT_ERROR -1
#define KV_UPDATE_SUCCESS 0
#define KV_PUT_SUCCESS 1

ABSL_FLAG(uint16_t, port, 50051, "Server port for the service");

class kv_storeImpl final : public kv_store::Service {

	private: 
		const char *db_name = "test.db";
		DatabaseUtils &db = DatabaseUtils::get_instance(db_name);

	Status get(ServerContext* context, const getReq* request, getResp* response) override {
		std::cout << "server finding value for key: " << request->key() << std::endl;
		
		// Sanity checks for get
		if (request->key().length() > MAX_KEY_LEN) {
			response->set_value ("");
			response->set_status(KV_KEY_ERROR);
			return Status::OK;
		}

		const char *key = request->key().c_str();
		std::cout << "Reaching here: " << key << std::endl;
		std::string value = db.get_value(key);
		std::cout << "Value from get_internal: " << value << std::endl;
		if (value == "") {
			// Key Not found
			std::cout << "Value for key: " << key << " not found\n";
			response->set_value(value);
			response->set_status(KV_GET_FAILED);
		} else {
			// Key found!!
			std::cout << "Value: " << value << "for key: " << key << "\n";
			response->set_value(value);
			response->set_status(KV_GET_SUCCESS);
		}
		
		return Status::OK;		
	}
	
	Status put(ServerContext* context, const putReq* request, putResp* response) override {
		std::cout << "server adding key to database: " << request->key() << std::endl;

		// Sanity checks for key and value
		if (request->key().length() > MAX_KEY_LEN || request->value().length() > MAX_VALUE_LEN) {
			response->set_old_value("");
			response->set_status(KV_PUT_ERROR);
			return Status::OK;
		}

		const char *key = request->key().c_str();
		const char *value = request->value().c_str();
		std::string old_value = db.put_value(key, value);

		if (old_value == "") {
			// Key not found, inserting for the first time
			std::cout << "Key: " << key << " inserted. Value: " << value << "\n";
			response->set_old_value(old_value);
			response->set_status(KV_PUT_SUCCESS);
		} else {
			std::cout << "Key: " << key << " updated. Current value: " << value << "\n";
			response->set_old_value(old_value);
			response->set_status(KV_UPDATE_SUCCESS);
		}

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
