add_executable(test 
               test.cc
                )

# Include the directory for headers
target_include_directories(test PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_include_directories(test PUBLIC "${CMAKE_LIB_DIR}")

target_link_libraries(test 
                      739kv
                     )

set_target_properties(test PROPERTIES
    ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}"
)
