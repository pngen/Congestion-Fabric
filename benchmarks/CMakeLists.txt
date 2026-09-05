add_executable(cf_bench bench.cpp)
target_link_libraries(cf_bench PRIVATE CongestionFabric::CongestionFabric ws2_32)
