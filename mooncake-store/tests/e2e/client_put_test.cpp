#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <iomanip>
#include <random>
#include <string>
#include <thread>

#include "client_wrapper.h"
#include "types.h"

// Command line flags
DEFINE_string(hostname, "localhost:9001", "Local hostname for the client");
DEFINE_string(metadata_server, "P2PHANDSHAKE",
              "Metadata server connection string. Use 'P2PHANDSHAKE' for P2P mode "
              "(no HTTP metadata server required), or 'http://IP:PORT/metadata' for HTTP mode");
DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "", "Device name to use, valid if protocol=rdma");
DEFINE_string(master_server_entry, "127.0.0.1:50051",
              "Master server entry (IP:Port)");
DEFINE_int32(segment_size_mb, 128, "Segment size in MB");
DEFINE_int32(value_size, 1024, "Value size in bytes (default: 1KB)");
DEFINE_int32(operation_interval_microsec, 1,
             "Interval between operations (GET+PUT) in microseconds");
DEFINE_int32(max_operations, 0,
             "Maximum number of operations (0 = unlimited)");
DEFINE_int32(key_space_size, 1000,
             "Key space size (number of unique keys to choose from). "
             "Smaller values lead to higher hit rates as more keys are written");
DEFINE_int32(stats_report_interval, 10,
             "Report statistics every N operations (0 = disable periodic reports)");

namespace mooncake {
namespace testing {

class ClientPutTester {
   public:
    ClientPutTester()
        : gen_(std::random_device{}()),
          key_dist_(0, 999),  // Will be reinitialized in Initialize()
          total_requests_(0),
          get_hits_(0),
          get_misses_(0),
          put_count_(0),
          put_success_(0),
          put_failures_(0) {}

    bool Initialize() {
        // Reinitialize key distribution with the actual key space size
        // (after gflags parsing)
        key_dist_ = std::uniform_int_distribution<>(0, FLAGS_key_space_size - 1);

        // Create client wrapper
        auto client_opt = ClientTestWrapper::CreateClientWrapper(
            FLAGS_hostname, FLAGS_metadata_server, FLAGS_protocol,
            FLAGS_device_name, FLAGS_master_server_entry);

        if (!client_opt.has_value()) {
            LOG(ERROR) << "Failed to create client wrapper";
            return false;
        }

        client_ = client_opt.value();
        LOG(INFO) << "Successfully created client wrapper";

        // Mount a segment
        size_t segment_size = static_cast<size_t>(FLAGS_segment_size_mb) * 1024 * 1024;
        void* buffer = nullptr;
        ErrorCode mount_err = client_->Mount(segment_size, buffer);
        if (mount_err != ErrorCode::OK) {
            LOG(ERROR) << "Failed to mount segment: " << toString(mount_err);
            return false;
        }

        segment_buffer_ = buffer;
        LOG(INFO) << "Successfully mounted segment: size=" << segment_size
                  << " bytes (" << FLAGS_segment_size_mb << " MB)";

        return true;
    }

    void Run() {
        LOG(INFO) << "Starting inference simulation test:";
        LOG(INFO) << "  Operation interval: " << FLAGS_operation_interval_microsec
                  << " us";
        LOG(INFO) << "  Value size: " << FLAGS_value_size << " bytes";
        LOG(INFO) << "  Key space size: " << FLAGS_key_space_size
                  << " (smaller = higher hit rate as data accumulates)";
        LOG(INFO) << "  Max operations: "
                  << (FLAGS_max_operations > 0 ? std::to_string(FLAGS_max_operations)
                                               : "unlimited");
        LOG(INFO) << "  Stats report interval: "
                  << (FLAGS_stats_report_interval > 0
                          ? std::to_string(FLAGS_stats_report_interval)
                          : "disabled");

        auto start_time = std::chrono::steady_clock::now();

        while (true) {
            // Generate a random key from the key space
            std::string key = GenerateRandomKey();

            // Step 1: Try GET first (simulate inference query)
            std::string retrieved_value;
            ErrorCode get_err = client_->Get(key, retrieved_value);

            total_requests_++;

            if (get_err == ErrorCode::OK) {
                // GET hit: key exists, no need to PUT
                get_hits_++;
                VLOG(1) << "[REQUEST #" << total_requests_ << "] GET HIT: key=" << key
                        << ", value_size=" << retrieved_value.size();
            } else if (get_err == ErrorCode::OBJECT_NOT_FOUND) {
                // GET miss: key doesn't exist, need to PUT
                get_misses_++;
                std::string new_value = GenerateRandomValue();

                // Step 2: Perform PUT operation
                ErrorCode put_err = client_->Put(key, new_value);
                put_count_++;

                if (put_err != ErrorCode::OK) {
                    put_failures_++;
                    LOG(WARNING) << "[REQUEST #" << total_requests_
                                 << "] GET MISS -> PUT FAILED: key=" << key
                                 << ", value_size=" << new_value.size()
                                 << ", error=" << toString(put_err);
                } else {
                    put_success_++;
                    VLOG(1) << "[REQUEST #" << total_requests_
                            << "] GET MISS -> PUT SUCCESS: key=" << key
                            << ", value_size=" << new_value.size();
                }
            } else {
                // GET error (other than NOT_FOUND)
                LOG(WARNING) << "[REQUEST #" << total_requests_
                             << "] GET ERROR: key=" << key
                             << ", error=" << toString(get_err);
            }

            // Report statistics periodically
            if (FLAGS_stats_report_interval > 0 &&
                total_requests_ % FLAGS_stats_report_interval == 0) {
                ReportStatistics(start_time);
            }

            // Check if we've reached the maximum number of operations
            if (FLAGS_max_operations > 0 &&
                total_requests_ >= FLAGS_max_operations) {
                LOG(INFO) << "Reached maximum operations limit: " << total_requests_;
                break;
            }

            // Sleep for the specified interval
            std::this_thread::sleep_for(
                std::chrono::microseconds(FLAGS_operation_interval_microsec));
        }

        // Final statistics report
        auto end_time = std::chrono::steady_clock::now();
        auto total_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            end_time - start_time);
        LOG(INFO) << "========================================";
        LOG(INFO) << "Test completed:";
        LOG(INFO) << "  Total time: " << total_elapsed.count() << "s";
        ReportStatistics(start_time);
    }

   private:
    void ReportStatistics(const std::chrono::steady_clock::time_point& start_time) {
        auto now = std::chrono::steady_clock::now();
        auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(now - start_time);

        double hit_rate = 0.0;
        if (total_requests_ > 0) {
            hit_rate = (static_cast<double>(get_hits_) / total_requests_) * 100.0;
        }

        double put_success_rate = 0.0;
        if (put_count_ > 0) {
            put_success_rate = (static_cast<double>(put_success_) / put_count_) * 100.0;
        }

        LOG(INFO) << "========================================";
        LOG(INFO) << "Statistics (after " << total_requests_ << " requests, "
                  << elapsed.count() << "s elapsed):";
        LOG(INFO) << "  Total requests: " << total_requests_;
        LOG(INFO) << "  GET hits: " << get_hits_ << " ("
                  << std::fixed << std::setprecision(2) << hit_rate << "%)";
        LOG(INFO) << "  GET misses: " << get_misses_;
        LOG(INFO) << "  PUT attempts: " << put_count_;
        LOG(INFO) << "  PUT success: " << put_success_ << " ("
                  << std::fixed << std::setprecision(2) << put_success_rate << "%)";
        LOG(INFO) << "  PUT failures: " << put_failures_;
        LOG(INFO) << "  Unique keys written: ~" << put_success_
                  << " (out of " << FLAGS_key_space_size << " key space)";
        LOG(INFO) << "========================================";
    }

    std::string GenerateRandomKey() {
        // Generate a random key from the key space: test_key_<0 to key_space_size-1>
        // This ensures that as more keys are written, the hit rate increases
        int key_index = key_dist_(gen_);
        return "test_key_" + std::to_string(key_index);
    }

    std::string GenerateRandomValue() {
        // Generate a random value of the specified size
        std::string value;
        value.reserve(FLAGS_value_size);

        // Fill with random alphanumeric characters
        const char charset[] =
            "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        const size_t charset_size = sizeof(charset) - 1;

        std::uniform_int_distribution<> char_dist(0, charset_size - 1);
        for (int i = 0; i < FLAGS_value_size; ++i) {
            value += charset[char_dist(gen_)];
        }

        return value;
    }

    std::shared_ptr<ClientTestWrapper> client_;
    void* segment_buffer_{nullptr};
    std::mt19937 gen_;
    std::uniform_int_distribution<> key_dist_;

    // Statistics
    std::atomic<int64_t> total_requests_;
    std::atomic<int64_t> get_hits_;
    std::atomic<int64_t> get_misses_;
    std::atomic<int64_t> put_count_;
    std::atomic<int64_t> put_success_;
    std::atomic<int64_t> put_failures_;
};

}  // namespace testing
}  // namespace mooncake

int main(int argc, char* argv[]) {
    google::InitGoogleLogging(argv[0]);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_logtostderr = 1;

    mooncake::testing::ClientPutTester tester;

    if (!tester.Initialize()) {
        LOG(ERROR) << "Failed to initialize client tester";
        return -1;
    }

    tester.Run();

    return 0;
}

