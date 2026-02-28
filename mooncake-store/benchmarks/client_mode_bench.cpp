/**
 * Benchmark comparing Mooncake Store embedded mode vs standalone mode.
 *
 * Embedded mode:  Client::Create() runs in-process with full TransferEngine.
 * Standalone mode: DummyClient proxies to external RealClient via RPC+SHM.
 *
 * Measures per-operation Put/Get latency (P50/P90/P95/P99) and throughput
 * across multiple value sizes, with warmup and multi-threading support.
 *
 * Usage:
 *   # Embedded mode
 *   ./client_mode_bench --mode=embedded --protocol=tcp
 *
 *   # Standalone mode (requires running mooncake_client on same host)
 *   ./client_mode_bench --mode=standalone --real_client_address=127.0.0.1:50052
 */

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <fstream>
#include <memory>
#include <numeric>
#include <span>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "allocator.h"
#include "client_service.h"
#include "dummy_client.h"
#include "types.h"
#include "utils.h"

// Mode selection
DEFINE_string(mode, "embedded", "Client mode: embedded or standalone");

// Value sizes (comma-separated)
DEFINE_string(value_sizes, "1024,65536,1048576,4194304,16777216",
              "Comma-separated list of value sizes in bytes");

// Operation counts
DEFINE_int32(warmup_ops, 10, "Number of warmup operations per thread");
DEFINE_int32(ops_per_thread, 100, "Number of operations per thread");
DEFINE_int32(num_threads, 1, "Number of concurrent worker threads");

// Network / protocol
DEFINE_string(protocol, "tcp", "Transfer protocol: tcp or rdma");
DEFINE_string(device_name, "", "RDMA device name (if protocol=rdma)");
DEFINE_string(master_address, "localhost:50051", "Master server address");
DEFINE_string(metadata_connection_string, "http://127.0.0.1:8080/metadata",
              "Metadata connection string");
DEFINE_string(local_hostname, "localhost:12345",
              "Local hostname for embedded client");

// Memory
DEFINE_uint64(ram_buffer_size_gb, 4,
              "RAM segment size in GB (embedded mode)");
DEFINE_uint64(client_buffer_allocator_size_mb, 256,
              "Client buffer allocator size in MB (embedded mode)");

// Standalone-specific
DEFINE_string(real_client_address, "127.0.0.1:50052",
              "RealClient RPC address for standalone mode");
DEFINE_uint64(dummy_mem_pool_size_mb, 3200,
              "DummyClient mem pool size in MB");
DEFINE_uint64(dummy_local_buffer_size_mb, 512,
              "DummyClient local buffer size in MB");

// Output
DEFINE_string(output_json, "",
              "Optional path to write JSON results");

namespace mooncake {
namespace benchmark {

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

static std::vector<size_t> ParseValueSizes(const std::string& s) {
    std::vector<size_t> out;
    std::stringstream ss(s);
    std::string tok;
    while (std::getline(ss, tok, ',')) {
        out.push_back(std::stoull(tok));
    }
    return out;
}

static std::string FormatBytes(size_t bytes) {
    const char* units[] = {"B", "KB", "MB", "GB"};
    int idx = 0;
    double val = static_cast<double>(bytes);
    while (val >= 1024 && idx < 3) {
        val /= 1024;
        idx++;
    }
    char buf[64];
    snprintf(buf, sizeof(buf), "%.1f%s", val, units[idx]);
    return buf;
}

// --------------------------------------------------------------------------
// OperationResult / ThreadStats
// --------------------------------------------------------------------------

struct OperationResult {
    double latency_us;
    bool is_put;
    bool success;
};

struct ThreadStats {
    std::vector<OperationResult> operations;
    uint64_t total_ops = 0;
    uint64_t success_ops = 0;
    uint64_t put_ops = 0;
    uint64_t get_ops = 0;
};

static void CalcPercentiles(std::vector<double>& lat, double& p50,
                            double& p90, double& p95, double& p99) {
    if (lat.empty()) {
        p50 = p90 = p95 = p99 = 0;
        return;
    }
    std::sort(lat.begin(), lat.end());
    auto n = lat.size();
    p50 = lat[std::min<size_t>(static_cast<size_t>(std::ceil(n * 0.50)) - 1, n - 1)];
    p90 = lat[std::min<size_t>(static_cast<size_t>(std::ceil(n * 0.90)) - 1, n - 1)];
    p95 = lat[std::min<size_t>(static_cast<size_t>(std::ceil(n * 0.95)) - 1, n - 1)];
    p99 = lat[std::min<size_t>(static_cast<size_t>(std::ceil(n * 0.99)) - 1, n - 1)];
}

// --------------------------------------------------------------------------
// Embedded benchmark
// --------------------------------------------------------------------------

struct EmbeddedContext {
    std::shared_ptr<Client> client;
    std::unique_ptr<SimpleAllocator> allocator;
    void* segment_ptr = nullptr;
    size_t segment_size = 0;
};

static bool InitEmbedded(EmbeddedContext& ctx) {
    auto opt = Client::Create(FLAGS_local_hostname,
                              FLAGS_metadata_connection_string,
                              FLAGS_protocol,
                              FLAGS_device_name.empty()
                                  ? std::nullopt
                                  : std::optional<std::string>(FLAGS_device_name),
                              FLAGS_master_address);
    if (!opt.has_value()) {
        LOG(ERROR) << "Client::Create failed";
        return false;
    }
    ctx.client = *opt;

    ctx.segment_size = FLAGS_ram_buffer_size_gb * 1024ull * 1024 * 1024;
    ctx.segment_ptr = allocate_buffer_allocator_memory(ctx.segment_size);
    if (!ctx.segment_ptr) {
        LOG(ERROR) << "Failed to allocate segment";
        return false;
    }
    auto res = ctx.client->MountSegment(ctx.segment_ptr, ctx.segment_size);
    if (!res.has_value()) {
        LOG(ERROR) << "MountSegment failed: " << toString(res.error());
        return false;
    }

    auto buf_size = FLAGS_client_buffer_allocator_size_mb * 1024 * 1024;
    ctx.allocator = std::make_unique<SimpleAllocator>(buf_size);
    auto reg = ctx.client->RegisterLocalMemory(
        ctx.allocator->getBase(), buf_size, "cpu:0", false, false);
    if (!reg.has_value()) {
        LOG(ERROR) << "RegisterLocalMemory failed: " << toString(reg.error());
        return false;
    }
    return true;
}

static void CleanupEmbedded(EmbeddedContext& ctx) {
    if (ctx.segment_ptr && ctx.client) {
        ctx.client->UnmountSegment(ctx.segment_ptr, ctx.segment_size);
    }
    ctx.client.reset();
    ctx.allocator.reset();
}

static void EmbeddedWorker(int tid, size_t value_size,
                           EmbeddedContext& ctx,
                           std::atomic<bool>& stop,
                           ThreadStats& stats,
                           bool warmup_only) {
    void* buf = ctx.allocator->allocate(value_size);
    if (!buf) {
        LOG(ERROR) << "Thread " << tid << ": allocate failed";
        return;
    }
    memset(buf, 'A' + (tid % 26), value_size);

    std::vector<Slice> slices;
    slices.emplace_back(Slice{buf, value_size});
    ReplicateConfig config;
    config.replica_num = 1;

    int ops = warmup_only ? FLAGS_warmup_ops : FLAGS_ops_per_thread;
    std::string prefix = warmup_only ? "warmup_" : "";

    // Phase 1: Put
    for (int i = 0; i < ops && !stop.load(); i++) {
        std::string key = prefix + "key_" + std::to_string(tid) + "_" +
                          std::to_string(value_size) + "_" + std::to_string(i);
        auto t0 = std::chrono::high_resolution_clock::now();
        auto res = ctx.client->Put(key.data(), slices, config);
        auto t1 = std::chrono::high_resolution_clock::now();
        double us = std::chrono::duration_cast<std::chrono::microseconds>(
                        t1 - t0).count();
        bool ok = res.has_value();
        if (!warmup_only) {
            stats.operations.push_back({us, true, ok});
            stats.total_ops++;
            if (ok) { stats.success_ops++; stats.put_ops++; }
        }
    }

    // Phase 2: Get
    for (int i = 0; i < ops && !stop.load(); i++) {
        std::string key = prefix + "key_" + std::to_string(tid) + "_" +
                          std::to_string(value_size) + "_" + std::to_string(i);
        auto t0 = std::chrono::high_resolution_clock::now();
        auto res = ctx.client->Get(key.data(), slices);
        auto t1 = std::chrono::high_resolution_clock::now();
        double us = std::chrono::duration_cast<std::chrono::microseconds>(
                        t1 - t0).count();
        bool ok = res.has_value();
        if (!warmup_only) {
            stats.operations.push_back({us, false, ok});
            stats.total_ops++;
            if (ok) { stats.success_ops++; stats.get_ops++; }
        }
    }

    ctx.allocator->deallocate(buf, value_size);
}

// --------------------------------------------------------------------------
// Standalone benchmark
// --------------------------------------------------------------------------

struct StandaloneContext {
    std::unique_ptr<DummyClient> client;
    void* buf = nullptr;
    size_t buf_size = 0;
};

static bool InitStandalone(StandaloneContext& ctx) {
    ctx.client = std::make_unique<DummyClient>();
    auto port_str = FLAGS_real_client_address.substr(
        FLAGS_real_client_address.find(':') + 1);
    std::string ipc_path = "@mooncake_client_" + port_str + ".sock";

    int rc = ctx.client->setup_dummy(
        FLAGS_dummy_mem_pool_size_mb * 1024 * 1024,
        FLAGS_dummy_local_buffer_size_mb * 1024 * 1024,
        FLAGS_real_client_address, ipc_path);
    if (rc != 0) {
        LOG(ERROR) << "DummyClient setup_dummy failed: " << rc;
        return false;
    }
    return true;
}

static void CleanupStandalone(StandaloneContext& ctx) {
    if (ctx.client) {
        ctx.client->tearDownAll();
    }
    ctx.client.reset();
}

static void StandaloneWorker(int tid, size_t value_size,
                             StandaloneContext& ctx,
                             std::atomic<bool>& stop,
                             ThreadStats& stats,
                             bool warmup_only) {
    int ops = warmup_only ? FLAGS_warmup_ops : FLAGS_ops_per_thread;
    std::string prefix = warmup_only ? "warmup_" : "";

    // Each op uses its own region within the pre-allocated buffer,
    // offset = i * value_size, to avoid memory conflicts between ops.
    char* base = static_cast<char*>(ctx.buf);

    // Phase 1: Put
    for (int i = 0; i < ops && !stop.load(); i++) {
        char* op_buf = base + (size_t)i * value_size;
        memset(op_buf, 'A' + (tid % 26), value_size);

        std::string key = prefix + "skey_" + std::to_string(tid) + "_" +
                          std::to_string(value_size) + "_" + std::to_string(i);
        std::span<const char> val(op_buf, value_size);
        auto t0 = std::chrono::high_resolution_clock::now();
        int ret = ctx.client->put(key, val);
        auto t1 = std::chrono::high_resolution_clock::now();
        double us = std::chrono::duration_cast<std::chrono::microseconds>(
                        t1 - t0).count();
        bool ok = (ret == 0);
        if (!warmup_only) {
            stats.operations.push_back({us, true, ok});
            stats.total_ops++;
            if (ok) { stats.success_ops++; stats.put_ops++; }
        }
    }

    // Phase 2: Get
    for (int i = 0; i < ops && !stop.load(); i++) {
        std::string key = prefix + "skey_" + std::to_string(tid) + "_" +
                          std::to_string(value_size) + "_" + std::to_string(i);
        auto t0 = std::chrono::high_resolution_clock::now();
        auto handle = ctx.client->get_buffer(key);
        auto t1 = std::chrono::high_resolution_clock::now();
        double us = std::chrono::duration_cast<std::chrono::microseconds>(
                        t1 - t0).count();
        bool ok = (handle != nullptr);
        if (!warmup_only) {
            stats.operations.push_back({us, false, ok});
            stats.total_ops++;
            if (ok) { stats.success_ops++; stats.get_ops++; }
        }
    }
}

// --------------------------------------------------------------------------
// Print results
// --------------------------------------------------------------------------

struct ValueSizeResult {
    size_t value_size;
    double duration_s;
    // Aggregated latencies
    double put_p50, put_p90, put_p95, put_p99;
    double get_p50, get_p90, get_p95, get_p99;
    double put_mean, get_mean;
    uint64_t put_ops, get_ops;
    double put_ops_s, get_ops_s;
    double put_mbps, get_mbps;
    uint64_t total_ops, success_ops;
};

static ValueSizeResult AggregateStats(
    size_t value_size,
    const std::vector<ThreadStats>& all_stats,
    double duration_s) {
    ValueSizeResult r{};
    r.value_size = value_size;
    r.duration_s = duration_s;

    std::vector<double> put_lat, get_lat;
    for (auto& ts : all_stats) {
        r.total_ops += ts.total_ops;
        r.success_ops += ts.success_ops;
        r.put_ops += ts.put_ops;
        r.get_ops += ts.get_ops;
        for (auto& op : ts.operations) {
            if (!op.success) continue;
            if (op.is_put)
                put_lat.push_back(op.latency_us);
            else
                get_lat.push_back(op.latency_us);
        }
    }

    CalcPercentiles(put_lat, r.put_p50, r.put_p90, r.put_p95, r.put_p99);
    CalcPercentiles(get_lat, r.get_p50, r.get_p90, r.get_p95, r.get_p99);

    if (!put_lat.empty())
        r.put_mean = std::accumulate(put_lat.begin(), put_lat.end(), 0.0) /
                     put_lat.size();
    if (!get_lat.empty())
        r.get_mean = std::accumulate(get_lat.begin(), get_lat.end(), 0.0) /
                     get_lat.size();

    r.put_ops_s = r.put_ops / duration_s;
    r.get_ops_s = r.get_ops / duration_s;
    r.put_mbps = (r.put_ops * value_size) / (duration_s * 1024.0 * 1024.0);
    r.get_mbps = (r.get_ops * value_size) / (duration_s * 1024.0 * 1024.0);

    return r;
}

static void PrintResult(const ValueSizeResult& r) {
    LOG(INFO) << "";
    LOG(INFO) << "=== " << FormatBytes(r.value_size)
              << " (duration " << r.duration_s << "s) ===";
    LOG(INFO) << "Total ops: " << r.total_ops
              << "  Success: " << r.success_ops;
    LOG(INFO) << "PUT ops: " << r.put_ops << "  ops/s: " << r.put_ops_s
              << "  MB/s: " << r.put_mbps;
    LOG(INFO) << "  Mean: " << r.put_mean << "us  P50: " << r.put_p50
              << "  P90: " << r.put_p90 << "  P95: " << r.put_p95
              << "  P99: " << r.put_p99;
    LOG(INFO) << "GET ops: " << r.get_ops << "  ops/s: " << r.get_ops_s
              << "  MB/s: " << r.get_mbps;
    LOG(INFO) << "  Mean: " << r.get_mean << "us  P50: " << r.get_p50
              << "  P90: " << r.get_p90 << "  P95: " << r.get_p95
              << "  P99: " << r.get_p99;
}

static void WriteJson(const std::vector<ValueSizeResult>& results,
                      const std::string& path) {
    std::ofstream f(path);
    f << "[\n";
    for (size_t i = 0; i < results.size(); i++) {
        auto& r = results[i];
        f << "  {\n"
          << "    \"value_size\": " << r.value_size << ",\n"
          << "    \"duration_s\": " << r.duration_s << ",\n"
          << "    \"put\": {"
          << "\"ops\": " << r.put_ops
          << ", \"ops_s\": " << r.put_ops_s
          << ", \"mbps\": " << r.put_mbps
          << ", \"mean_us\": " << r.put_mean
          << ", \"p50_us\": " << r.put_p50
          << ", \"p90_us\": " << r.put_p90
          << ", \"p95_us\": " << r.put_p95
          << ", \"p99_us\": " << r.put_p99
          << "},\n"
          << "    \"get\": {"
          << "\"ops\": " << r.get_ops
          << ", \"ops_s\": " << r.get_ops_s
          << ", \"mbps\": " << r.get_mbps
          << ", \"mean_us\": " << r.get_mean
          << ", \"p50_us\": " << r.get_p50
          << ", \"p90_us\": " << r.get_p90
          << ", \"p95_us\": " << r.get_p95
          << ", \"p99_us\": " << r.get_p99
          << "}\n"
          << "  }" << (i + 1 < results.size() ? "," : "") << "\n";
    }
    f << "]\n";
    f.close();
    LOG(INFO) << "JSON results saved to " << path;
}

}  // namespace benchmark
}  // namespace mooncake

// --------------------------------------------------------------------------
// main
// --------------------------------------------------------------------------

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;

    using namespace mooncake::benchmark;

    auto value_sizes = ParseValueSizes(FLAGS_value_sizes);

    LOG(INFO) << "=== Mooncake Client Mode Benchmark ===";
    LOG(INFO) << "Mode: " << FLAGS_mode;
    LOG(INFO) << "Protocol: " << FLAGS_protocol;
    LOG(INFO) << "Threads: " << FLAGS_num_threads;
    LOG(INFO) << "Ops/thread: " << FLAGS_ops_per_thread;
    LOG(INFO) << "Warmup: " << FLAGS_warmup_ops;

    std::vector<ValueSizeResult> all_results;

    if (FLAGS_mode == "embedded") {
        EmbeddedContext ctx;
        if (!InitEmbedded(ctx)) return 1;

        for (size_t vs : value_sizes) {
            LOG(INFO) << "\n--- Benchmarking " << FormatBytes(vs) << " ---";

            // Check buffer allocator has enough space
            if (FLAGS_num_threads * vs >
                FLAGS_client_buffer_allocator_size_mb * 1024 * 1024) {
                LOG(WARNING) << "Skipping " << FormatBytes(vs)
                             << ": exceeds buffer allocator capacity";
                continue;
            }

            std::atomic<bool> stop{false};

            // Warmup
            {
                std::vector<std::thread> warmup_threads;
                std::vector<ThreadStats> warmup_stats(FLAGS_num_threads);
                for (int t = 0; t < FLAGS_num_threads; t++) {
                    warmup_threads.emplace_back(
                        EmbeddedWorker, t, vs, std::ref(ctx),
                        std::ref(stop), std::ref(warmup_stats[t]), true);
                }
                for (auto& th : warmup_threads) th.join();
                LOG(INFO) << "Warmup complete";
            }

            // Actual benchmark
            std::vector<std::thread> workers;
            std::vector<ThreadStats> stats(FLAGS_num_threads);
            auto t0 = std::chrono::high_resolution_clock::now();
            for (int t = 0; t < FLAGS_num_threads; t++) {
                workers.emplace_back(
                    EmbeddedWorker, t, vs, std::ref(ctx),
                    std::ref(stop), std::ref(stats[t]), false);
            }
            for (auto& w : workers) w.join();
            auto t1 = std::chrono::high_resolution_clock::now();
            double dur =
                std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0)
                    .count() / 1000.0;

            auto result = AggregateStats(vs, stats, dur);
            PrintResult(result);
            all_results.push_back(result);
        }

        CleanupEmbedded(ctx);

    } else if (FLAGS_mode == "standalone") {
        if (FLAGS_num_threads > 1) {
            LOG(WARNING)
                << "Multi-threaded standalone mode shares a single "
                   "DummyClient instance. Buffer registration from "
                   "concurrent threads may cause contention. "
                   "Forcing num_threads=1 for correctness.";
            FLAGS_num_threads = 1;
        }

        StandaloneContext ctx;
        if (!InitStandalone(ctx)) return 1;

        // Allocate a large buffer: max_ops * max_value_size so each op
        // gets its own memory region and avoids conflicts.
        size_t max_vs = *std::max_element(value_sizes.begin(),
                                          value_sizes.end());
        int max_ops = std::max((int)FLAGS_ops_per_thread,
                               (int)FLAGS_warmup_ops);
        size_t total_buf_size = (size_t)max_ops * max_vs;
        uint64_t buf_addr = ctx.client->alloc_from_mem_pool(total_buf_size);
        if (!buf_addr) {
            LOG(ERROR) << "alloc_from_mem_pool failed for "
                       << total_buf_size << " bytes ("
                       << max_ops << " ops * " << FormatBytes(max_vs) << ")";
            return 1;
        }
        ctx.buf = reinterpret_cast<void*>(buf_addr);
        ctx.buf_size = total_buf_size;

        int rc = ctx.client->register_buffer(ctx.buf, total_buf_size);
        if (rc != 0) {
            LOG(ERROR) << "register_buffer failed: " << rc;
            return 1;
        }
        LOG(INFO) << "Allocated standalone buffer: " << max_ops << " ops * "
                  << FormatBytes(max_vs) << " = "
                  << FormatBytes(total_buf_size);

        for (size_t vs : value_sizes) {
            LOG(INFO) << "\n--- Benchmarking " << FormatBytes(vs) << " ---";

            std::atomic<bool> stop{false};

            // Warmup (single-threaded for standalone due to shared DummyClient)
            {
                ThreadStats warmup_stats;
                StandaloneWorker(0, vs, ctx, stop, warmup_stats, true);
                LOG(INFO) << "Warmup complete";
            }

            // Actual benchmark
            // Note: DummyClient operations are thread-safe via internal mutex,
            // but for multi-thread we run sequentially from different "threads"
            // to avoid contention on the single DummyClient instance.
            std::vector<ThreadStats> stats(FLAGS_num_threads);
            auto t0 = std::chrono::high_resolution_clock::now();

            if (FLAGS_num_threads == 1) {
                StandaloneWorker(0, vs, ctx, stop, stats[0], false);
            } else {
                std::vector<std::thread> workers;
                for (int t = 0; t < FLAGS_num_threads; t++) {
                    workers.emplace_back(
                        StandaloneWorker, t, vs, std::ref(ctx),
                        std::ref(stop), std::ref(stats[t]), false);
                }
                for (auto& w : workers) w.join();
            }

            auto t1 = std::chrono::high_resolution_clock::now();
            double dur =
                std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0)
                    .count() / 1000.0;

            auto result = AggregateStats(vs, stats, dur);
            PrintResult(result);
            all_results.push_back(result);
        }

        ctx.client->unregister_buffer(ctx.buf);
        CleanupStandalone(ctx);

    } else {
        LOG(ERROR) << "Unknown mode: " << FLAGS_mode;
        return 1;
    }

    if (!FLAGS_output_json.empty()) {
        WriteJson(all_results, FLAGS_output_json);
    }

    LOG(INFO) << "\n=== Benchmark Complete ===";
    google::ShutdownGoogleLogging();
    return 0;
}
