# Copyright 2018 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# cmake build file for C++ helloworld example.
# Assumes protobuf and gRPC have been installed using cmake.
# See cmake_externalproject/CMakeLists.txt for all-in-one cmake build
# that automatically builds all the dependencies before building helloworld.

cmake_minimum_required(VERSION 3.8)

project(kv_store C CXX)

include(common.cmake)

# Proto file
get_filename_component(hw_proto "./kv_store.proto" ABSOLUTE)
get_filename_component(hw_proto_path "${hw_proto}" PATH)

# Generated sources
set(hw_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/kv_store.pb.cc")
set(hw_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/kv_store.pb.h")
set(hw_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/kv_store.grpc.pb.cc")
set(hw_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/kv_store.grpc.pb.h")
add_custom_command(
      OUTPUT "${hw_proto_srcs}" "${hw_proto_hdrs}" "${hw_grpc_srcs}" "${hw_grpc_hdrs}"
      COMMAND ${_PROTOBUF_PROTOC}
      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
        -I "${hw_proto_path}"
        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
        "${hw_proto}"
      DEPENDS "${hw_proto}")

# Include generated *.pb.h files
include_directories("${CMAKE_CURRENT_BINARY_DIR}")

# Enable -fPIC for all targets (especially important for shared libraries)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

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

add_library(utils utils.cc)

target_link_libraries(utils)

# Create a library from client.cc
add_library(739kv SHARED 739kv.cc client.cc)

# Link necessary libraries to the 738kv
target_link_libraries(739kv
  utils
  hw_grpc_proto
  absl::check
  absl::flags
  absl::flags_parse
  absl::log
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

# Create the server executable
add_executable(server server.cc)
target_link_libraries(server
  hw_grpc_proto
  absl::check
  absl::flags
  absl::flags_parse
  absl::log
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

# Create the test executable and link it with the client library
add_executable(test test.cc)
target_link_libraries(test
  utils
  739kv  # Link the client library
  hw_grpc_proto
  absl::check
  absl::flags
  absl::flags_parse
  absl::log
  ${_REFLECTION}
  ${_GRPC_GRPCPP}
  ${_PROTOBUF_LIBPROTOBUF})

## Targets greeter_[async_](client|server)
#foreach(_target
#  client server test)
#  #greeter_callback_client greeter_callback_server
#  #greeter_async_client greeter_async_client2 greeter_async_server)
#  add_executable(${_target} "${_target}.cc")
#  target_link_libraries(${_target}
#    hw_grpc_proto
#    absl::check
#    absl::flags
#    absl::flags_parse
#    absl::log
#    ${_REFLECTION}
#    ${_GRPC_GRPCPP}
#    ${_PROTOBUF_LIBPROTOBUF})
#endforeach()
