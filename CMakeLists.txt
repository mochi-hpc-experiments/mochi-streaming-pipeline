cmake_minimum_required(VERSION 3.15)
project(mochi-streaming-pipeline CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Build type
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Compiler flags
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0 -Wall -Wextra")
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")

# Find packages
find_package(MPI REQUIRED)
find_package(thallium REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(yaml-cpp REQUIRED)

pkg_check_modules(ABT_IO REQUIRED IMPORTED_TARGET abt-io)

# Common library
add_library(pipeline_common STATIC
    src/common/config.cpp
    src/common/buffer_pool.cpp
)
target_include_directories(pipeline_common PUBLIC
    ${CMAKE_SOURCE_DIR}/src
)
target_link_libraries(pipeline_common PUBLIC
    thallium
    yaml-cpp::yaml-cpp
)

# Provider libraries
add_library(pipeline_sender STATIC
    src/sender/sender_provider.cpp
)
target_link_libraries(pipeline_sender PUBLIC
    pipeline_common
)

add_library(pipeline_broker STATIC
    src/broker/broker_provider.cpp
)
target_link_libraries(pipeline_broker PUBLIC
    pipeline_common
    PkgConfig::ABT_IO
)

add_library(pipeline_receiver STATIC
    src/receiver/receiver_provider.cpp
)
target_link_libraries(pipeline_receiver PUBLIC
    pipeline_common
)

# Main executable
add_executable(pipeline_benchmark
    src/main.cpp
)
target_link_libraries(pipeline_benchmark PRIVATE
    pipeline_sender
    pipeline_broker
    pipeline_receiver
    MPI::MPI_CXX
)

# Install
install(TARGETS pipeline_benchmark
    RUNTIME DESTINATION bin
)
install(DIRECTORY configs/
    DESTINATION share/mochi-streaming-pipeline/configs
)

# Tests (optional)
option(BUILD_TESTS "Build unit tests" ON)
if(BUILD_TESTS)
    enable_testing()
    find_package(Catch2 QUIET)
    if(Catch2_FOUND)
        add_subdirectory(tests)
    else()
        message(STATUS "Catch2 not found, skipping tests")
    endif()
endif()
