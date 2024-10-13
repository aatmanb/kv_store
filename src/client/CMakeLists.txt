# Add the client sources
add_library(client 
            client.cc
            )

# Include the directory for headers
target_include_directories(client PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

target_link_libraries(client
                        pthread 
                        kv_store_grpc_proto 
                        boost_thread
                        absl::check absl::flags absl::flags_parse absl::log
                        ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF})

set_target_properties(client PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}"
)
