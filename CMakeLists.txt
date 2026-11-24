# SPDX-License-Identifier: Apache-2.0
cmake_minimum_required(VERSION 3.16)
project(shardnet VERSION 0.1.0 LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS ON)          # gnu11: epoll, accept4, recvmmsg
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

option(SN_WERROR "Treat warnings as errors" ON)
set(SN_SANITIZE "" CACHE STRING "Sanitizers, e.g. address,undefined or thread")

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(FATAL_ERROR "shardnet requires Linux (epoll, SO_REUSEPORT, recvmmsg)")
endif()

add_compile_options(-Wall -Wextra -Wshadow -Wformat=2 -Wstrict-prototypes
                    -Wmissing-prototypes -Wvla -Wpointer-arith)
if(SN_WERROR)
  add_compile_options(-Werror)
endif()
if(SN_SANITIZE)
  add_compile_options(-fsanitize=${SN_SANITIZE} -fno-omit-frame-pointer
                      -fno-sanitize-recover=all)
  add_link_options(-fsanitize=${SN_SANITIZE})
endif()

find_package(Threads REQUIRED)

add_library(shardnet STATIC
  src/server.c src/buf.c src/pool.c src/hist.c src/log.c)
target_include_directories(shardnet
  PUBLIC  ${PROJECT_SOURCE_DIR}/include
  PRIVATE ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(shardnet PUBLIC Threads::Threads)

add_executable(snserver apps/snserver.c)
target_link_libraries(snserver PRIVATE shardnet)

add_executable(snbench apps/snbench.c)
target_link_libraries(snbench PRIVATE shardnet m)

enable_testing()
foreach(t test_buf test_pool test_hist test_server)
  add_executable(${t} tests/${t}.c)
  target_include_directories(${t} PRIVATE ${PROJECT_SOURCE_DIR}/src
                                          ${PROJECT_SOURCE_DIR}/tests)
  target_link_libraries(${t} PRIVATE shardnet m)
  add_test(NAME ${t} COMMAND ${t})
  set_tests_properties(${t} PROPERTIES TIMEOUT 300)
endforeach()

# End-to-end: real server process + load generator over loopback.
add_test(NAME e2e_bench
  COMMAND ${PROJECT_SOURCE_DIR}/scripts/e2e.sh $<TARGET_FILE:snserver>
          $<TARGET_FILE:snbench>)
set_tests_properties(e2e_bench PROPERTIES TIMEOUT 120)
