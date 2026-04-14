cmake_minimum_required(VERSION 3.11)
project(kudu-ingest-cpp)

# Set C++ standard
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find Kudu client library
# You may need to set KUDU_HOME environment variable or specify the path
find_path(KUDU_INCLUDE_DIR kudu/client/client.h
  HINTS
    $ENV{KUDU_HOME}/include
    /usr/include
    /usr/local/include
    /opt/kudu/include
)

find_library(KUDU_CLIENT_LIB kudu_client
  HINTS
    $ENV{KUDU_HOME}/lib
    /usr/lib
    /usr/lib/x86_64-linux-gnu
    /usr/local/lib64
    /usr/local/lib
    /opt/kudu/lib
)

if(NOT KUDU_INCLUDE_DIR OR NOT KUDU_CLIENT_LIB)
  message(FATAL_ERROR "Kudu client library not found. Please set KUDU_HOME environment variable or install Kudu client library.")
endif()

message(STATUS "Found Kudu include directory: ${KUDU_INCLUDE_DIR}")
message(STATUS "Found Kudu client library: ${KUDU_CLIENT_LIB}")

# Include directories
include_directories(${KUDU_INCLUDE_DIR})

# Prometheus client (pull endpoint)
include(FetchContent)
set(ENABLE_PULL ON CACHE BOOL "" FORCE)
set(ENABLE_PUSH ON CACHE BOOL "" FORCE)
FetchContent_Declare(
  prometheus-cpp
  GIT_REPOSITORY https://github.com/jupp0r/prometheus-cpp.git
  GIT_TAG v1.3.0
)
FetchContent_MakeAvailable(prometheus-cpp)

# Executable
add_executable(kudu_insert kudu_insert.cc)

# Link libraries
target_link_libraries(kudu_insert ${KUDU_CLIENT_LIB} prometheus-cpp::pull
                      prometheus-cpp::push)

# Enable warnings
if(CMAKE_COMPILER_IS_GNUCXX OR CMAKE_CXX_COMPILER_ID MATCHES "Clang")
  target_compile_options(kudu_insert PRIVATE -Wall -Wextra)
endif()

# Installation
install(TARGETS kudu_insert DESTINATION bin)

# Print build information
message(STATUS "")
message(STATUS "Build Configuration:")
message(STATUS "  C++ Compiler: ${CMAKE_CXX_COMPILER}")
message(STATUS "  C++ Standard: ${CMAKE_CXX_STANDARD}")
message(STATUS "  Build Type: ${CMAKE_BUILD_TYPE}")
message(STATUS "")
