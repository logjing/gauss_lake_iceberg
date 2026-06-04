# dual_table - Dual Table Architecture Plugin for openGauss
# ustore internal table + Iceberg foreign table auto-sync
cmake_minimum_required(VERSION 3.15)
project(dual_table VERSION 1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# openGauss paths
if(NOT DEFINED OPENGAUSS_HOME)
    set(OPENGAUSS_HOME "/home/sin/openGauss-server")
endif()
if(NOT DEFINED OPENGAUSS_INSTALL)
    set(OPENGAUSS_INSTALL "${OPENGAUSS_HOME}/mppdb_temp_install")
endif()

set(OPENGAUSS_INCLUDE "${OPENGAUSS_INSTALL}/include")
set(OPENGAUSS_SERVER_INCLUDE "${OPENGAUSS_INSTALL}/include/postgresql/server")

# Include directories - openGauss
include_directories(BEFORE ${OPENGAUSS_INCLUDE})
include_directories(BEFORE ${OPENGAUSS_SERVER_INCLUDE})

# Arrow library paths (optional - for Parquet I/O)
if(DEFINED ARROW_HOME)
    set(ARROW_INCLUDE "${ARROW_HOME}/include")
    set(ARROW_LIB "${ARROW_HOME}/lib")
    include_directories(BEFORE ${ARROW_INCLUDE})
    link_directories(${ARROW_LIB})
    set(ARROW_LIBS arrow parquet)
else()
    set(ARROW_LIBS "")
endif()

# Source files - force C++ compilation
file(GLOB SOURCES "src/*.cpp")
foreach(src ${SOURCES})
    set_source_files_properties(${src} PROPERTIES LANGUAGE CXX)
endforeach()

# Build shared library (.so)
add_library(dual_table SHARED ${SOURCES})
set_target_properties(dual_table PROPERTIES
    LINKER_LANGUAGE CXX
    PREFIX ""
    OUTPUT_NAME "dual_table"
    LINK_FLAGS "-Wl,--no-as-needed"
)

# openGauss MACRO_OPTIONS - required for correct struct layouts and ABI compatibility
# IMPORTANT: Override PG_VERSION_NUM to match runtime server (9.2.x) not header (13.0)
target_compile_options(dual_table PRIVATE
    -UPG_VERSION_NUM
)
target_compile_definitions(dual_table PRIVATE
    PGXC
    ENABLE_GSTRACE
    _GNU_SOURCE
    STREAMPLAN
    _THREAD_SAFE
    _REENTRANT
    _POSIX_PTHREAD_SEMANTICS
    USE_SPQ
    PG_VERSION_NUM=90204
)

target_compile_options(dual_table PRIVATE
    -Wall
    -fPIC
    -O2
    -Wno-unused-parameter
    -Wno-unused-variable
    -Wno-deprecated
    -Wno-unused-but-set-variable
    -Wno-sign-compare
)

# Link Arrow libraries if available
if(DEFINED ARROW_HOME)
    target_link_libraries(dual_table ${ARROW_LIBS})
endif()

# Set RPATH to use system libstdc++
set_target_properties(dual_table PROPERTIES
    BUILD_RPATH "/usr/lib64"
    INSTALL_RPATH "/usr/lib64"
)

# Install
install(TARGETS dual_table LIBRARY DESTINATION lib/postgresql)
install(FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/dual_table.control
    ${CMAKE_CURRENT_SOURCE_DIR}/dual_table--1.0.sql
    DESTINATION share/postgresql/extension
)

message(STATUS "=== Dual Table Plugin v1.0 ===")
message(STATUS "openGauss include: ${OPENGAUSS_INCLUDE}")
message(STATUS "openGauss server include: ${OPENGAUSS_SERVER_INCLUDE}")