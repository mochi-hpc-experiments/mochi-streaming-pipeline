# Integration tests for the Mochi Streaming Pipeline
#
# Each .yaml file in this directory becomes a test case.
# Tests run: mpirun -np 3 ./pipeline_benchmark <test.yaml>
# Success = exit code 0

# Find MPI for mpirun/mpiexec
find_package (MPI REQUIRED)

# Get path to the benchmark executable
set (BENCHMARK_EXE $<TARGET_FILE:pipeline_benchmark>)

# Find all YAML test configuration files in this directory
# Exclude spack.yaml which is used for CI environment setup
file (GLOB TEST_YAML_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.yaml")
list (FILTER TEST_YAML_FILES EXCLUDE REGEX "spack\\.yaml$")

# Create a test for each YAML file
foreach (YAML_FILE ${TEST_YAML_FILES})
    # Extract filename without path and extension for test name
    get_filename_component (TEST_NAME ${YAML_FILE} NAME_WE)

    # Add the test
    add_test (
        NAME integration_${TEST_NAME}
        COMMAND ${MPIEXEC_EXECUTABLE}
                ${MPIEXEC_NUMPROC_FLAG} 3
                ${MPIEXEC_PREFLAGS}
                ${BENCHMARK_EXE}
                ${YAML_FILE}
                ${MPIEXEC_POSTFLAGS}
    )

    # Set test properties
    set_tests_properties (integration_${TEST_NAME} PROPERTIES
        TIMEOUT 120
        PROCESSORS 3
        LABELS "integration"
    )
endforeach ()

# Report how many tests were found
list (LENGTH TEST_YAML_FILES NUM_TESTS)
message (STATUS "Found ${NUM_TESTS} integration test(s) in ${CMAKE_CURRENT_SOURCE_DIR}")
