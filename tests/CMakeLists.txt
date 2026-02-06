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

# Python unit tests for the search module
find_package (Python3 COMPONENTS Interpreter)
if (Python3_FOUND)
    # Check if pytest is available
    execute_process (
        COMMAND ${Python3_EXECUTABLE} -c "import pytest"
        RESULT_VARIABLE PYTEST_NOT_FOUND
        OUTPUT_QUIET
        ERROR_QUIET
    )

    if (NOT PYTEST_NOT_FOUND)
        add_test (
            NAME python_search_unit_tests
            COMMAND ${Python3_EXECUTABLE} -m pytest
                    ${CMAKE_CURRENT_SOURCE_DIR}/unit/test_search.py
                    -v
            WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        )

        set_tests_properties (python_search_unit_tests PROPERTIES
            TIMEOUT 60
            LABELS "unit;python"
        )

        message (STATUS "Added Python unit tests (pytest found)")
    else ()
        message (STATUS "Skipping Python unit tests (pytest not found)")
    endif ()
else ()
    message (STATUS "Skipping Python unit tests (Python3 not found)")
endif ()
