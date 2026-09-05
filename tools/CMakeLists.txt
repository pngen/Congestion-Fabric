
add_executable(cfcoordinator ${CMAKE_CURRENT_SOURCE_DIR}/../src/coordinator.cpp)
target_link_libraries(cfcoordinator PRIVATE CongestionFabric::CongestionFabric ws2_32)

add_executable(cfcli cfcli.cpp)
target_compile_features(cfcli PRIVATE cxx_std_20)
target_include_directories(cfcli PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(cfcli PRIVATE CongestionFabric::CongestionFabric)

add_executable(cfworker cfworker.cpp)
target_compile_features(cfworker PRIVATE cxx_std_20)
target_include_directories(cfworker PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../include)
target_link_libraries(cfworker PRIVATE ws2_32)
