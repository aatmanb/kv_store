# Define the library name
add_library(739kv SHARED
    739kv.cc
)

# Include directories for public headers
target_include_directories(739kv PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CLIENT_DIR}
    ${CMAKE_UTILS_DIR}
)

set_target_properties(739kv PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}"
)


include_directories("${CMAKE_SOURCE_DIR}/bin")

target_link_libraries(739kv
                        pthread 
                        kv_store_grpc_proto 
                        boost_thread
                        absl::check absl::flags absl::flags_parse absl::log
                        ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF}
                        client)
                        
## Add subdirectories
add_subdirectory(client)
add_subdirectory(server)
add_subdirectory(utils)
add_subdirectory(proto)


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

#cmake_minimum_required(VERSION 3.8)
#
#project(kv_store C CXX)
#
##set(CMAKE_C_COMPILER /usr/bin/gcc-12)
#set(CMAKE_C_COMPILER /usr/bin/gcc)
#set(CMAKE_CXX_STANDARD 17)
#set(CMAKE_CXX_STANDARD_REQUIRED True)
#set(CMAKE_POSITION_INDEPENDENT_CODE ON)
#
#include(./proto/common.cmake)
#
## Proto file
#get_filename_component(hw_proto "./proto/kv_store.proto" ABSOLUTE)
#get_filename_component(hw_proto_path "${hw_proto}" PATH)
#
## Generated sources
#set(hw_proto_srcs "${CMAKE_CURRENT_BINARY_DIR}/kv_store.pb.cc")
#set(hw_proto_hdrs "${CMAKE_CURRENT_BINARY_DIR}/kv_store.pb.h")
#set(kv_store_grpc_srcs "${CMAKE_CURRENT_BINARY_DIR}/kv_store.grpc.pb.cc")
#set(kv_store_grpc_hdrs "${CMAKE_CURRENT_BINARY_DIR}/kv_store.grpc.pb.h")
#add_custom_command(
#      OUTPUT "${hw_proto_srcs}" "${hw_proto_hdrs}" "${kv_store_grpc_srcs}" "${kv_store_grpc_hdrs}"
#      COMMAND ${_PROTOBUF_PROTOC}
#      ARGS --grpc_out "${CMAKE_CURRENT_BINARY_DIR}"
#        --cpp_out "${CMAKE_CURRENT_BINARY_DIR}"
#        -I "${hw_proto_path}"
#        --plugin=protoc-gen-grpc="${_GRPC_CPP_PLUGIN_EXECUTABLE}"
#        "${hw_proto}"
#      DEPENDS "${hw_proto}")
#
## Include generated *.pb.h files
#include_directories("${CMAKE_CURRENT_BINARY_DIR}")
## Include tsl 
#include_directories("${PROJECT_SOURCE_DIR}/../external")
#include_directories("${PROJECT_SOURCE_DIR}/../extern/pybind11/include")
#include_directories("/usr/include/python3.10")
## message(STATUS "Project root directory: ${PROJECT_SOURCE_DIR}")
#
## kv_store_grpc_proto
#add_library(kv_store_grpc_proto
#  ${kv_store_grpc_srcs}
#  ${kv_store_grpc_hdrs}
#  ${hw_proto_srcs}
#  ${hw_proto_hdrs})
#target_link_libraries(kv_store_grpc_proto
#  absl::check
#  ${_REFLECTION}
#  ${_GRPC_GRPCPP}
#  ${_PROTOBUF_LIBPROTOBUF})
#
#add_library(sqlite3 ../external/sqlite3/sqlite3.c)
#
#add_library(739kv SHARED
#            server.h server.cpp 
#            data.h data.cpp dbutils.h
#            singleton.h partition_manager.h
#            client.h client.cc
#            739kv.h 739kv.cc
#            utils.h utils.cc)
#
#target_link_libraries(739kv pthread kv_store_grpc_proto boost_thread
#                      absl::check absl::flags
#                      absl::flags_parse absl::log sqlite3
#                      ${_REFLECTION} ${_GRPC_GRPCPP} ${_PROTOBUF_LIBPROTOBUF})
#
#
#target_include_directories(739kv PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
#
#target_include_directories(739kv PRIVATE ${PROJECT_SOURCE_DIR}/external)
#
#add_executable(server main.cpp)
#
#target_link_libraries(server 739kv)
#
#add_dependencies(server 739kv)
#add_dependencies(739kv sqlite3)
#
#add_executable(test test.cc)
#target_link_libraries(test 739kv)
#add_dependencies(test 739kv)
#
#add_library(kv739_py SHARED wrapper.cpp)
#target_link_libraries(kv739_py PRIVATE 739kv)
#add_dependencies(kv739_py 739kv)
## message(STATUS "${PROJECT_SOURCE_DIR}/../extern/pybind11/tools")
## set(pybind11_DIR "${PROJECT_SOURCE_DIR}/../extern/pybind11/tools")
## set(PYBIND11_FINDPYTHON ON)
## # set(PYBIND11_TEST OFF)
## find_package(pybind11 CONFIG REQUIRED)
#
## pybind11_add_module(739kv_py wrapper.cpp)
## target_link_libraries(739kv_py PRIVATE 739kv)
## add_dependencies(739kv_py 739kv)
## install(TARGETS 739kv_py DESTINATION .)
