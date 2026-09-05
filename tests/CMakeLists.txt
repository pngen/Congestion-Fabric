
# Congestion Fabric tests. No test timeouts anywhere.
add_executable(cf_smoke smoke.cpp)
target_link_libraries(cf_smoke PRIVATE CongestionFabric::CongestionFabric)
add_test(NAME cf_smoke COMMAND cf_smoke)

add_executable(cf_accounting accounting.cpp)
target_link_libraries(cf_accounting PRIVATE CongestionFabric::CongestionFabric)
add_test(NAME cf_accounting COMMAND cf_accounting)

add_executable(cf_persistence persistence.cpp)
target_link_libraries(cf_persistence PRIVATE CongestionFabric::CongestionFabric)
add_test(NAME cf_persistence COMMAND cf_persistence)

add_executable(cf_physical physical_traffic.cpp)
target_link_libraries(cf_physical PRIVATE CongestionFabric::CongestionFabric ws2_32)
add_test(NAME cf_physical COMMAND cf_physical)

add_executable(cf_property property.cpp)
target_link_libraries(cf_property PRIVATE CongestionFabric::CongestionFabric)
add_test(NAME cf_property COMMAND cf_property)

add_executable(cf_concurrency concurrency.cpp)
target_link_libraries(cf_concurrency PRIVATE CongestionFabric::CongestionFabric)
add_test(NAME cf_concurrency COMMAND cf_concurrency)

add_executable(cf_adversarial adversarial.cpp)
target_link_libraries(cf_adversarial PRIVATE CongestionFabric::CongestionFabric)
add_test(NAME cf_adversarial COMMAND cf_adversarial)

# Real OS-process multiprocess proof + coordinator restart (framed TCP).
find_program(POWERSHELL_EXEC powershell)
if(POWERSHELL_EXEC AND CONGFABRIC_BUILD_TOOLS)
  add_test(NAME cf_multiprocess
    COMMAND ${POWERSHELL_EXEC} -NoProfile -ExecutionPolicy Bypass -File ${CMAKE_CURRENT_SOURCE_DIR}/multiprocess.ps1
            -ToolsDir ${CMAKE_BINARY_DIR}/tools -WorkDir ${CMAKE_BINARY_DIR}/mp)
  add_test(NAME cf_coordinator_restart
    COMMAND ${POWERSHELL_EXEC} -NoProfile -ExecutionPolicy Bypass -File ${CMAKE_CURRENT_SOURCE_DIR}/coordinator_restart.ps1
            -ToolsDir ${CMAKE_BINARY_DIR}/tools -WorkDir ${CMAKE_BINARY_DIR}/restart)
endif()

if(CONGFABRIC_BUILD_CUDA_PROOFS AND POWERSHELL_EXEC)
  add_test(NAME cf_cuda_death
    COMMAND ${POWERSHELL_EXEC} -NoProfile -ExecutionPolicy Bypass -File ${CMAKE_CURRENT_SOURCE_DIR}/cuda_death.ps1
            -CudaDir ${CMAKE_BINARY_DIR}/cuda -WorkDir ${CMAKE_BINARY_DIR}/cudadeath)
  if(CONGFABRIC_BUILD_TOOLS)
    add_test(NAME cf_cuda_integrated
      COMMAND ${POWERSHELL_EXEC} -NoProfile -ExecutionPolicy Bypass -File ${CMAKE_CURRENT_SOURCE_DIR}/cuda_integrated.ps1
              -ToolsDir ${CMAKE_BINARY_DIR}/tools -CudaDir ${CMAKE_BINARY_DIR}/cuda -WorkDir ${CMAKE_BINARY_DIR}/cudaint)
  endif()
endif()

if(CONGFABRIC_ENABLE_ASAN)
  foreach(t cf_smoke cf_accounting cf_persistence cf_physical cf_property cf_concurrency cf_adversarial)
    target_compile_options(${t} PRIVATE /fsanitize=address /Zi)
    target_link_options(${t} PRIVATE /fsanitize=address)
  endforeach()
endif()
