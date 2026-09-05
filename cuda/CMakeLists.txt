
# RTX 5090 (sm_120) CUDA transfer proofs. These are real host<->device evidence
# always labeled MEASURED; derived queue/congestion state is labeled DERIVED.
set(CMAKE_CUDA_ARCHITECTURES 120)
add_executable(cu_proof cu_proof.cu)
target_compile_options(cu_proof PRIVATE --allow-unsupported-compiler)
target_link_libraries(cu_proof PRIVATE CongestionFabric::CongestionFabric)

add_executable(cu_worker cu_worker.cu)
target_compile_options(cu_worker PRIVATE --allow-unsupported-compiler)
target_link_libraries(cu_worker PRIVATE CongestionFabric::CongestionFabric ws2_32)

if(CONGFABRIC_ENABLE_ASAN)
  # CUDA + ASan is not a supported combination for the MSVC/nvcc host toolchain.
  message(STATUS "CUDA proofs built without ASan (CUDA host code is not ASan-compatible).")
endif()
