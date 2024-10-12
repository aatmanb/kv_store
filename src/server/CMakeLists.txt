add_executable(server 
                main.cpp
                server.cpp
                data.cpp
                dbutils.h
                singleton.h
                partition_manager.h
                thread_safe_queue.h
                request.h
                )

# Include the directory for headers
target_include_directories(server PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(server PUBLIC "${CMAKE_SOURCE_DIR}/external")

target_link_libraries(server 
                        pthread 
                        kv_store_grpc_proto 
                        boost_thread
                        absl::check absl::flags absl::flags_parse absl::log
                        sqlite3
                        ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF})

set_target_properties(server PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}"
)
