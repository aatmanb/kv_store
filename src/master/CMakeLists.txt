add_executable(master
                main.cpp
                master.h
                master.cpp
                replication.h
                replication.cpp)

# Include the directory for headers
target_include_directories(master PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/..")
target_include_directories(master PUBLIC "${CMAKE_SOURCE_DIR}/external")

target_link_libraries(master 
                        pthread 
                        kv_store_grpc_proto 
                        boost_thread
                        absl::check absl::flags absl::flags_parse absl::log
                        sqlite3
                        ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF})