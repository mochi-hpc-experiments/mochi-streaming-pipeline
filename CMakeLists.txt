cmake_minimum_required (VERSION 3.15)
project (mochi-streaming-pipeline CXX)

set (CMAKE_CXX_STANDARD 17)
set (CMAKE_CXX_STANDARD_REQUIRED ON)
set (CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Build type
if (NOT CMAKE_BUILD_TYPE)
    set (CMAKE_BUILD_TYPE Release)
endif ()

# Compiler flags
set (CMAKE_CXX_FLAGS_DEBUG "-g -O0 -Wall -Wextra")
set (CMAKE_CXX_FLAGS_RELEASE "-O3 -DNDEBUG")

# Coverage configuration
option (ENABLE_COVERAGE "Enable code coverage" OFF)
add_library (coverage_config INTERFACE)

if (ENABLE_COVERAGE AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  message (STATUS "Code coverage enabled")
  target_compile_options (coverage_config INTERFACE
      -O0        # no optimization
      -g         # generate debug info
      --coverage # sets all required flags
  )
  if (CMAKE_VERSION VERSION_GREATER_EQUAL 3.13)
    target_link_options (coverage_config INTERFACE --coverage)
  else ()
    target_link_libraries (coverage_config INTERFACE --coverage)
  endif ()
endif ()

# Find packages
find_package (MPI REQUIRED)
find_package (thallium REQUIRED)
find_package (PkgConfig REQUIRED)
find_package (yaml-cpp REQUIRED)

pkg_check_modules (ABT_IO REQUIRED IMPORTED_TARGET abt-io)

# Common library
add_library (pipeline_common STATIC
    src/common/config.cpp
    src/common/buffer_pool.cpp
)
target_include_directories (pipeline_common PUBLIC
    ${CMAKE_SOURCE_DIR}/src
)
target_link_libraries (pipeline_common PUBLIC
    thallium
    yaml-cpp::yaml-cpp
    PRIVATE coverage_config
)

# Provider libraries
add_library (pipeline_sender STATIC
    src/sender/sender_provider.cpp
)
target_link_libraries (pipeline_sender PUBLIC
    pipeline_common
    PRIVATE coverage_config
)

add_library (pipeline_broker STATIC
    src/broker/broker_provider.cpp
)
target_link_libraries (pipeline_broker PUBLIC
    pipeline_common
    PkgConfig::ABT_IO
    PRIVATE coverage_config
)

add_library (pipeline_receiver STATIC
    src/receiver/receiver_provider.cpp
)
target_link_libraries (pipeline_receiver PUBLIC
    pipeline_common
    PRIVATE coverage_config
)

# Main executable
add_executable (pipeline_benchmark
    src/main.cpp
)
target_link_libraries (pipeline_benchmark PRIVATE
    pipeline_sender
    pipeline_broker
    pipeline_receiver
    MPI::MPI_CXX
    PRIVATE coverage_config
)

# Install
install (TARGETS pipeline_benchmark
    RUNTIME DESTINATION bin
)
install (DIRECTORY configs/
    DESTINATION share/mochi-streaming-pipeline/configs
)

# Tests (optional)
option (BUILD_TESTS "Build tests" ON)
if (BUILD_TESTS)
    enable_testing ()
    add_subdirectory (tests)
endif ()
