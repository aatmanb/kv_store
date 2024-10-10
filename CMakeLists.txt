cmake_minimum_required(VERSION 3.8)
project(kv_store C CXX)

#set(CMAKE_C_COMPILER /usr/bin/gcc-12)
set(CMAKE_C_COMPILER /usr/bin/gcc)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Set the output directories for compiled binaries
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/bin)
set(CMAKE_CLIENT_DIR ${CMAKE_SOURCE_DIR}/src/client)
set(CMAKE_SERVER_DIR ${CMAKE_SOURCE_DIR}/src/server)
set(CMAKE_UTILS_DIR ${CMAKE_SOURCE_DIR}/src/utils)

# Find absl package
find_package(absl CONFIG REQUIRED)

# Add subdirectories
add_subdirectory(src)
#add_subdirectory(test)
