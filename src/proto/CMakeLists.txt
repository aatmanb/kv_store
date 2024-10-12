include(common.cmake)

# kv_store service

# Proto file
get_filename_component(kv_store "./kv_store.proto" ABSOLUTE)
get_filename_component(kv_store_path "${kv_store}" PATH)

# Generated sources
set(kv_store_srcs "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/kv_store.pb.cc")
set(kv_store_hdrs "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/kv_store.pb.h")
set(kv_store_grpc_srcs "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/kv_store.grpc.pb.cc")
set(kv_store_grpc_hdrs "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/kv_store.grpc.pb.h")
add_custom_command(
      OUTPUT "${kv_store_srcs}" "${kv_store_hdrs}" "${kv_store_grpc_srcs}" "${kv_store_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}"
        --cpp_out "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}"
        -I "${kv_store_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${kv_store}"
      DEPENDS "${kv_store}")

# Include generated *.pb.h files
include_directories("${CMAKE_RUNTIME_OUTPUT_DIRECTORY}")

# kv_store_grpc_proto
add_library(kv_store_grpc_proto
  ${kv_store_grpc_srcs}
  ${kv_store_grpc_hdrs}
  ${kv_store_srcs}
  ${kv_store_hdrs})
target_link_libraries(kv_store_grpc_proto
  absl::check
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})




# node service

# Proto file
get_filename_component(node "./node.proto" ABSOLUTE)
get_filename_component(node_path "${node}" PATH)

# Generated sources
set(node_srcs "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/node.pb.cc")
set(node_hdrs "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/node.pb.h")
set(node_grpc_srcs "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/node.grpc.pb.cc")
set(node_grpc_hdrs "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/node.grpc.pb.h")
add_custom_command(
      OUTPUT "${node_srcs}" "${node_hdrs}" "${node_grpc_srcs}" "${node_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}"
        --cpp_out "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}"
        -I "${node_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${node}"
      DEPENDS "${node}")

# Include generated *.pb.h files
include_directories("${CMAKE_RUNTIME_OUTPUT_DIRECTORY}")

# kv_store_grpc_proto
add_library(node_grpc_proto
  ${node_grpc_srcs}
  ${node_grpc_hdrs}
  ${node_srcs}
  ${node_hdrs})
target_link_libraries(node_grpc_proto
  absl::check
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})
