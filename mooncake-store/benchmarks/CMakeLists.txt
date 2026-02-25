# Add allocator benchmark executable
add_executable(allocator_bench allocator_bench.cpp)
target_link_libraries(allocator_bench PRIVATE cachelib_memory_allocator mooncake_store)

# Add master benchmark executable
add_executable(master_bench master_bench.cpp)
target_link_libraries(master_bench PRIVATE cachelib_memory_allocator mooncake_store)

# Add file interface benchmark executable
add_executable(file_interface_bench file_interface_bench.cpp)
target_link_libraries(file_interface_bench PRIVATE mooncake_store glog::glog)
# Add storage backend benchmark executable
# This benchmark tests all storage backends (OffsetAllocator, Bucket, FilePerKey)
# with realistic KV cache workloads for LLM inference scenarios
add_executable(storage_backend_bench storage_backend_bench.cpp)
target_link_libraries(storage_backend_bench PRIVATE
    mooncake_store
    cachelib_memory_allocator
    gflags::gflags
    glog::glog
    pthread
)

# Add client mode comparison benchmark executable
# Compares embedded (in-process Client) vs standalone (DummyClient+RealClient)
add_executable(client_mode_bench client_mode_bench.cpp)
target_link_libraries(client_mode_bench PRIVATE
    mooncake_store
    transfer_engine
    cachelib_memory_allocator
    mooncake_common
    glog::glog
    gflags::gflags
    pthread
)
