cmake_minimum_required(VERSION 3.16)
project(MapReduce VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wpedantic")

add_library(mapreduce STATIC
    src/MapReduceFramework.cpp
    src/JobContext.cpp
    src/Barrier.cpp
    src/internal/Phases.cpp
)

target_include_directories(mapreduce
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)

# --- GoogleTest ---
include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
    DOWNLOAD_EXTRACT_TIMESTAMP TRUE
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
set(BUILD_GMOCK OFF CACHE BOOL "" FORCE)
set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_executable(mapreduce_tests
    tests/barrier_test.cpp
    tests/job_lifecycle_test.cpp
    tests/correctness_count_test.cpp
    tests/empty_input_test.cpp
    tests/multi_thread_test.cpp
    tests/concurrent_jobs_test.cpp
    tests/progress_test.cpp
    tests/golden_regression_test.cpp
    tests/test_helpers.cpp
)

target_link_libraries(mapreduce_tests PRIVATE mapreduce GTest::gtest_main)
target_include_directories(mapreduce_tests PRIVATE tests ${CMAKE_SOURCE_DIR}/src)
target_compile_definitions(mapreduce_tests PRIVATE MAPREDUCE_SOURCE_DIR="${CMAKE_SOURCE_DIR}")

include(GoogleTest)
gtest_discover_tests(mapreduce_tests)

# --- Examples ---
add_executable(word_count examples/word_count/main.cpp)
target_link_libraries(word_count PRIVATE mapreduce)

add_executable(char_frequency examples/char_frequency/main.cpp)
target_link_libraries(char_frequency PRIVATE mapreduce)

# --- Benchmark ---
add_executable(bench benchmarks/throughput_bench.cpp)
target_link_libraries(bench PRIVATE mapreduce)
