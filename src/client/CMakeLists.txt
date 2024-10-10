# Add the client sources
add_library(client 
            client.cc)

# Include the directory for headers
target_include_directories(client PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

set_target_properties(client PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}"
)
