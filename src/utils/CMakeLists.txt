# Add the utils sources
add_library(utils utils.cc)

# Include the directory for headers
target_include_directories(utils PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

## Link the utility library with kv_store_lib
#target_link_libraries(utils_lib kv_store_lib)

set_target_properties(utils PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}"    # For static libraries
)
