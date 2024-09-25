#
# Copyright 2015 gRPC authors.
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

# TODO(jtattermusch): Remove the hack to workaround protobuf bug. See https://github.com/protocolbuffers/protobuf/issues/12439
# Hack: protobuf currently doesn't declare it's absl dependencies when protobuf.pc pkgconfig file is used.
PROTOBUF_ABSL_DEPS = absl_absl_check absl_absl_log absl_algorithm absl_base absl_bind_front absl_bits absl_btree absl_cleanup absl_cord absl_core_headers absl_debugging absl_die_if_null absl_dynamic_annotations absl_flags absl_flat_hash_map absl_flat_hash_set absl_function_ref absl_hash absl_layout absl_log_initialize absl_log_severity absl_memory absl_node_hash_map absl_node_hash_set absl_optional absl_span absl_status absl_statusor absl_strings absl_synchronization absl_time absl_type_traits absl_utility absl_variant
# TODO(jtattermusch): Remove the hack to workaround protobuf/utf8_range bug. See https://github.com/protocolbuffers/utf8_range/issues/20
# Hack: utf8_range (which is protobuf's dependency) currently doesn't have a pkgconfig file, so we need to explicitly
# tweak the list of libraries to link against to fix the build.
PROTOBUF_UTF8_RANGE_LINK_LIBS = -lutf8_validity

HOST_SYSTEM = $(shell uname | cut -f 1 -d_)
SYSTEM ?= $(HOST_SYSTEM)
CXX = g++
CPPFLAGS += `pkg-config --cflags protobuf grpc absl_flags absl_flags_parse`
CXXFLAGS += -std=c++14
ifeq ($(SYSTEM),Darwin)
LDFLAGS += -L/usr/local/lib `pkg-config --libs --static protobuf grpc++ absl_flags absl_flags_parse $(PROTOBUF_ABSL_DEPS)`\
           $(PROTOBUF_UTF8_RANGE_LINK_LIBS) \
           -pthread\
           -lgrpc++_reflection\
           -ldl
else
LDFLAGS += -L/usr/local/lib `pkg-config --libs --static protobuf grpc++ absl_flags absl_flags_parse $(PROTOBUF_ABSL_DEPS)`\
           $(PROTOBUF_UTF8_RANGE_LINK_LIBS) \
           -pthread\
           -Wl,--no-as-needed -lgrpc++_reflection -Wl,--as-needed\
           -ldl
endif
PROTOC = protoc
GRPC_CPP_PLUGIN = grpc_cpp_plugin
GRPC_CPP_PLUGIN_PATH ?= `which $(GRPC_CPP_PLUGIN)`

PROTOS_PATH = $(pwd)

vpath %.proto $(PROTOS_PATH)

#all: system-check greeter_client greeter_server greeter_async_client greeter_async_client2 greeter_async_server greeter_callback_client greeter_callback_server
all: server
#client: helloworld.pb.o helloworld.grpc.pb.o greeter_client.o
#	$(CXX) $^ $(LDFLAGS) -o $@

server: kv_store.pb.o kv_store.grpc.pb.o server.o
	$(CXX) $^ -I/root/.local $(LDFLAGS) -o $@
