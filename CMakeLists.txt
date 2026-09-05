cmake_minimum_required(VERSION 3.20)
project(congestion_fabric VERSION 1.0.0 LANGUAGES CXX)

option(CONGFABRIC_BUILD_TESTS "Build Congestion Fabric tests" ON)
option(CONGFABRIC_BUILD_EXAMPLES "Build Congestion Fabric examples" ON)
option(CONGFABRIC_BUILD_TOOLS "Build Congestion Fabric tools" ON)
option(CONGFABRIC_BUILD_BENCHMARKS "Build Congestion Fabric benchmarks" ON)
option(CONGFABRIC_ENABLE_ASAN "Enable AddressSanitizer (MSVC)" OFF)
option(CONGFABRIC_BUILD_CUDA_PROOFS "Build RTX 5090 CUDA proofs" OFF)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release)
endif()

add_library(congestion_fabric STATIC
  src/engine.cpp
)
add_library(CongestionFabric::CongestionFabric ALIAS congestion_fabric)
set_target_properties(congestion_fabric PROPERTIES EXPORT_NAME CongestionFabric)

target_include_directories(congestion_fabric
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>)
target_compile_features(congestion_fabric PUBLIC cxx_std_20)

if(MSVC)
  target_compile_options(congestion_fabric PRIVATE /W4 /WX /MP)
  if(CONGFABRIC_ENABLE_ASAN)
    target_compile_options(congestion_fabric PRIVATE /fsanitize=address /Zi)
    target_link_options(congestion_fabric PRIVATE /fsanitize=address)
  endif()
elseif(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
  target_compile_options(congestion_fabric PRIVATE -Wall -Wextra -Wpedantic -Werror)
  if(CONGFABRIC_ENABLE_ASAN)
    target_compile_options(congestion_fabric PRIVATE -fsanitize=address -fno-omit-frame-pointer)
    target_link_options(congestion_fabric PRIVATE -fsanitize=address)
  endif()
endif()

if(CONGFABRIC_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()
if(CONGFABRIC_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()
if(CONGFABRIC_BUILD_TOOLS)
  add_subdirectory(tools)
endif()
if(CONGFABRIC_BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()
if(CONGFABRIC_BUILD_CUDA_PROOFS)
  enable_language(CUDA)
  add_subdirectory(cuda)
endif()

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

install(TARGETS congestion_fabric
  EXPORT CongestionFabricTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT CongestionFabricTargets
  FILE CongestionFabricTargets.cmake
  NAMESPACE CongestionFabric::
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/CongestionFabric)

configure_package_config_file(
  ${CMAKE_CURRENT_SOURCE_DIR}/cmake/CongestionFabricConfig.cmake.in
  ${CMAKE_CURRENT_BINARY_DIR}/CongestionFabricConfig.cmake
  INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/CongestionFabric)
write_basic_package_version_file(
  ${CMAKE_CURRENT_BINARY_DIR}/CongestionFabricConfigVersion.cmake
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion)
install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/CongestionFabricConfig.cmake
  ${CMAKE_CURRENT_BINARY_DIR}/CongestionFabricConfigVersion.cmake
  DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/CongestionFabric)
