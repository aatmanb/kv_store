include(common.cmake)

# Proto file
get_filename_component(hw_proto "./kv_store.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)

# Generated sources
set(hw_proto_srcs "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/kv_store.pb.cc")
set(hw_proto_hdrs "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/kv_store.pb.h")
set(hw_grpc_srcs "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/kv_store.grpc.pb.cc")
set(hw_grpc_hdrs "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/kv_store.grpc.pb.h")
add_custom_command(
      OUTPUT "${hw_proto_srcs}" "${hw_proto_hdrs}" "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}"
        --cpp_out "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}"
        -I "${hw_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${hw_proto}"
      DEPENDS "${hw_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_RUNTIME_OUTPUT_DIRECTORY}")

# hw_grpc_proto
add_library(hw_grpc_proto
  ${hw_grpc_srcs}
  ${hw_grpc_hdrs}
  ${hw_proto_srcs}
  ${hw_proto_hdrs})
target_link_libraries(hw_grpc_proto
  absl::check
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})
