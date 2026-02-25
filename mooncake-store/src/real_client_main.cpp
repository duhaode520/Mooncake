#include <gflags/gflags.h>
#include <csignal>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "client_service.h"
#include "default_config.h"
#include "real_client.h"

using namespace mooncake;

DEFINE_string(host, "0.0.0.0", "Local hostname");
DEFINE_string(metadata_server, "http://127.0.0.1:8080/metadata",
              "Metadata server connection string");
DEFINE_string(device_names, "", "Device names");
DEFINE_string(master_server_address, "127.0.0.1:50051",
              "Master server address");
DEFINE_string(protocol, "tcp", "Protocol");
DEFINE_int32(port, 50052, "Real Client service port");
DEFINE_string(global_segment_size, "4 GB", "Size of global segment");
DEFINE_int32(threads, 1, "Number of threads for client service");
DEFINE_bool(enable_offload, false, "Enable offload availability");
DEFINE_string(config_path, "", "Path to client config file (JSON)");

struct ClientConfig {
    std::string host = "0.0.0.0";
    std::string metadata_server = "http://127.0.0.1:8080/metadata";
    std::string device_names = "";
    std::string master_server_address = "127.0.0.1:50051";
    std::string protocol = "tcp";
    int32_t port = 50052;
    std::string global_segment_size = "4 GB";
    int32_t threads = 1;
    bool enable_offload = false;

    // Load values from a JSON/YAML config file. Keys not present in the file
    // fall back to gflags defaults (single source of truth for default values,
    // consistent with mooncake_master's InitMasterConf pattern).
    void LoadFromConfig(const mooncake::DefaultConfig& cfg) {
        cfg.GetString("host", &host, FLAGS_host);
        cfg.GetString("metadata_server", &metadata_server,
                      FLAGS_metadata_server);
        cfg.GetString("device_names", &device_names, FLAGS_device_names);
        cfg.GetString("master_server_address", &master_server_address,
                      FLAGS_master_server_address);
        cfg.GetString("protocol", &protocol, FLAGS_protocol);
        cfg.GetInt32("port", &port, FLAGS_port);
        cfg.GetString("global_segment_size", &global_segment_size,
                      FLAGS_global_segment_size);
        cfg.GetInt32("threads", &threads, FLAGS_threads);
        cfg.GetBool("enable_offload", &enable_offload, FLAGS_enable_offload);
    }
};

// Override config values with any CLI flags that were explicitly set.
// When no config file was provided (!conf_set), all FLAGS values are applied.
void ApplyCmdlineOverrides(ClientConfig& c, bool conf_set) {
    google::CommandLineFlagInfo info;
    auto overrides = [&](const char* name) -> bool {
        return (google::GetCommandLineFlagInfo(name, &info) &&
                !info.is_default) ||
               !conf_set;
    };
    if (overrides("host")) c.host = FLAGS_host;
    if (overrides("metadata_server")) c.metadata_server = FLAGS_metadata_server;
    if (overrides("device_names")) c.device_names = FLAGS_device_names;
    if (overrides("master_server_address"))
        c.master_server_address = FLAGS_master_server_address;
    if (overrides("protocol")) c.protocol = FLAGS_protocol;
    if (overrides("port")) c.port = FLAGS_port;
    if (overrides("global_segment_size"))
        c.global_segment_size = FLAGS_global_segment_size;
    if (overrides("threads")) c.threads = FLAGS_threads;
    if (overrides("enable_offload")) c.enable_offload = FLAGS_enable_offload;
}

namespace mooncake {
void RegisterClientRpcService(coro_rpc::coro_rpc_server& server,
                              RealClient& real_client) {
    server.register_handler<&RealClient::put_dummy_helper>(&real_client);
    server.register_handler<&RealClient::put_batch_dummy_helper>(&real_client);
    server.register_handler<&RealClient::put_parts_dummy_helper>(&real_client);
    server.register_handler<&RealClient::remove_internal>(&real_client);
    server.register_handler<&RealClient::removeByRegex_internal>(&real_client);
    server.register_handler<&RealClient::removeAll_internal>(&real_client);
    server.register_handler<&RealClient::isExist_internal>(&real_client);
    server.register_handler<&RealClient::batchIsExist_internal>(&real_client);
    server.register_handler<&RealClient::getSize_internal>(&real_client);
    server.register_handler<&RealClient::batch_put_from_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::batch_get_into_dummy_helper>(
        &real_client);
    server.register_handler<&RealClient::map_shm_internal>(&real_client);
    server.register_handler<&RealClient::unmap_shm_internal>(&real_client);
    server.register_handler<&RealClient::unregister_shm_buffer_internal>(
        &real_client);
    server.register_handler<&RealClient::service_ready_internal>(&real_client);
    server.register_handler<&RealClient::ping>(&real_client);
    server.register_handler<&RealClient::acquire_hot_cache>(&real_client);
    server.register_handler<&RealClient::release_hot_cache>(&real_client);
    server.register_handler<&RealClient::batch_acquire_hot_cache>(&real_client);
    server.register_handler<&RealClient::batch_release_hot_cache>(&real_client);
    server.register_handler<&RealClient::acquire_buffer_dummy>(&real_client);
    server.register_handler<&RealClient::release_buffer_dummy>(&real_client);
    server.register_handler<&RealClient::batch_acquire_buffer_dummy>(
        &real_client);
    server.register_handler<&RealClient::create_copy_task>(&real_client);
    server.register_handler<&RealClient::create_move_task>(&real_client);
    server.register_handler<&RealClient::query_task>(&real_client);
    server.register_handler<&RealClient::batch_get_offload_object>(
        &real_client);
}
}  // namespace mooncake

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    ClientConfig client_config;
    const bool has_config_file = !FLAGS_config_path.empty();
    if (has_config_file) {
        mooncake::DefaultConfig default_config;
        default_config.SetPath(FLAGS_config_path);
        try {
            default_config.Load();
        } catch (const std::exception& e) {
            LOG(FATAL) << "Failed to load client config: " << e.what();
            return 1;
        }
        client_config.LoadFromConfig(default_config);
    }
    ApplyCmdlineOverrides(client_config, has_config_file);

    size_t global_segment_size =
        string_to_byte_size(client_config.global_segment_size);

    auto client_inst = RealClient::create();
    auto res = client_inst->setup_internal(
        client_config.host, client_config.metadata_server, global_segment_size,
        0, client_config.protocol, client_config.device_names,
        client_config.master_server_address, nullptr,
        "@mooncake_client_" + std::to_string(client_config.port) + ".sock",
        client_config.port, client_config.enable_offload);
    if (!res) {
        LOG(FATAL) << "Failed to setup client: " << toString(res.error());
        return -1;
    }

    if (client_inst->start_dummy_client_monitor()) {
        LOG(FATAL) << "Failed to start dummy client monitor thread";
        return -1;
    }

    coro_rpc::coro_rpc_server server(client_config.threads, client_config.port,
                                     "127.0.0.1");
    RegisterClientRpcService(server, *client_inst);

    LOG(INFO) << "Client config: host=" << client_config.host
              << ", metadata_server=" << client_config.metadata_server
              << ", device_names=" << client_config.device_names
              << ", master_server_address="
              << client_config.master_server_address
              << ", protocol=" << client_config.protocol
              << ", port=" << client_config.port
              << ", global_segment_size="
              << client_config.global_segment_size
              << ", threads=" << client_config.threads
              << ", enable_offload=" << client_config.enable_offload;

    LOG(INFO) << "Starting real client service on 127.0.0.1:"
              << client_config.port;

    return server.start();
}
