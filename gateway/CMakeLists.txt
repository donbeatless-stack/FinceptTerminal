# Fincept market-data gateway (fmdg): a standalone C++20 service that takes in a
# market-data feed and fans it out to many TCP clients.
#
#   port 8589  long connections  — subscribe once, then ticks are pushed
#   port 8588  short connections — one text request, one reply, close
#
# Dependencies come from vcpkg (manifest: vcpkg.json). Configure with the vcpkg
# toolchain, e.g. `cmake --preset win` (CMakePresets.json).
cmake_minimum_required(VERSION 3.25)
project(fincept_market_data_gateway LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(FMDG_ASAN "Build with AddressSanitizer" OFF)
if(FMDG_ASAN)
    if(MSVC)
        add_compile_options(/fsanitize=address)
        # ASan cannot be combined with incremental linking or /RTC run-time checks.
        add_link_options(/INCREMENTAL:NO)
        foreach(cfg DEBUG RELWITHDEBINFO)
            string(REPLACE "/RTC1" "" CMAKE_CXX_FLAGS_${cfg} "${CMAKE_CXX_FLAGS_${cfg}}")
        endforeach()
    else()
        add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
        add_link_options(-fsanitize=address,undefined)
    endif()
endif()

find_package(asio CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(simdjson CONFIG REQUIRED)
find_package(PostgreSQL REQUIRED)
find_package(GTest CONFIG REQUIRED)

# Strict warnings on our own code only; dependency headers come in as SYSTEM
# includes from their imported targets.
function(fmdg_warnings target)
    if(MSVC)
        target_compile_options(${target} PRIVATE /W4 /permissive- /WX /utf-8)
    else()
        target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic -Werror)
    endif()
endfunction()

add_library(fmdg_core STATIC
    src/latency_histogram.cpp
    src/protocol.cpp
    src/conflating_queue.cpp
    src/symbol_table.cpp
    src/distributor.cpp
    src/push_session.cpp
    src/request_session.cpp
    src/gateway.cpp
    src/synthetic_feed.cpp
    src/ws_codec.cpp
    src/alpaca_protocol.cpp
    src/alpaca_feed.cpp
    src/tls_roots.cpp
    src/pg.cpp
    src/bar_store.cpp)
target_include_directories(fmdg_core PUBLIC include PRIVATE src)
target_link_libraries(fmdg_core PUBLIC asio::asio OpenSSL::SSL OpenSSL::Crypto simdjson::simdjson PostgreSQL::PostgreSQL)
if(WIN32)
    target_link_libraries(fmdg_core PUBLIC crypt32) # Windows root store for TLS verification
endif()
target_compile_definitions(fmdg_core PUBLIC ASIO_STANDALONE ASIO_NO_DEPRECATED)
if(WIN32)
    target_compile_definitions(fmdg_core PUBLIC _WIN32_WINNT=0x0A00 WIN32_LEAN_AND_MEAN NOMINMAX)
endif()
fmdg_warnings(fmdg_core)

add_executable(fmdg src/main.cpp)
target_link_libraries(fmdg PRIVATE fmdg_core)
fmdg_warnings(fmdg)

add_executable(fmdg_loadgen tools/loadgen.cpp)
target_link_libraries(fmdg_loadgen PRIVATE fmdg_core)
fmdg_warnings(fmdg_loadgen)

add_executable(fmdg_pg_bench tools/pg_bench.cpp)
target_link_libraries(fmdg_pg_bench PRIVATE fmdg_core)
fmdg_warnings(fmdg_pg_bench)

enable_testing()
add_executable(fmdg_tests
    tests/test_spsc_ring.cpp
    tests/test_latency_histogram.cpp
    tests/test_protocol.cpp
    tests/test_conflating_queue.cpp
    tests/test_symbol_table.cpp
    tests/test_distributor.cpp
    tests/test_gateway.cpp
    tests/test_ws_codec.cpp
    tests/test_alpaca_protocol.cpp
    tests/test_alpaca_feed.cpp
    tests/test_pool.cpp
    tests/test_bar_store.cpp)
target_link_libraries(fmdg_tests PRIVATE fmdg_core GTest::gtest_main)
fmdg_warnings(fmdg_tests)
include(GoogleTest)
gtest_discover_tests(fmdg_tests DISCOVERY_TIMEOUT 30)
