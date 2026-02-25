# Mooncake Store Client Mode Benchmark 测试指南

本指南描述如何使用 `client_mode_bench` 工具对比 Mooncake Store 的两种客户端部署模式的性能。

## 背景

| 模式 | 说明 | 典型场景 |
|------|------|---------|
| **Embedded** | `RealClient` 直接嵌入推理进程，调用 `store.setup()` | 简单部署，进程内直接访问 |
| **Standalone** | `DummyClient` 嵌入推理进程，通过 RPC+SHM 委托给独立的 `RealClient` 进程，调用 `store.setup_dummy()` | Worker 解耦部署（如 SGLang TP 场景） |

## 前置条件

### 编译

```bash
cd /path/to/Mooncake
mkdir -p build && cd build
cmake .. -DUSE_HTTP=ON -DBUILD_UNIT_TESTS=ON
make -j$(nproc)
sudo make install   # 安装 mooncake_master, mooncake_client 等二进制
```

### 安装 Python 包

```bash
./scripts/local_build.sh 3.12 dist-py312
pip install mooncake-wheel/dist-py312/*.whl
```

### 依赖确认

- `mooncake_master` 和 `mooncake_client` 二进制在 PATH 中
- Python 环境中可以 `from mooncake.store import MooncakeDistributedStore`

---

## 单节点测试

### 方式一：一键脚本

```bash
cd mooncake-store/benchmarks/

# 最简运行（TCP, 默认参数）
./run_client_mode_bench.sh

# 自定义参数
./run_client_mode_bench.sh \
    --protocol tcp \
    --value-sizes "65536,1048576,4194304" \
    --num-ops 200 \
    --warmup-ops 20 \
    --batch-size 4 \
    --num-threads 2 \
    --output-dir ./my_results

# 只跑 Python benchmark
./run_client_mode_bench.sh --skip-cpp

# 只跑 C++ benchmark
./run_client_mode_bench.sh --skip-python
```

脚本会自动：
1. 启动 `mooncake_master`
2. 运行 embedded 模式 benchmark
3. 重启 master + 启动 `mooncake_client`（RealClient）
4. 运行 standalone 模式 benchmark
5. 生成对比报告
6. 清理所有进程

### 方式二：手动分步执行

适用于需要精细控制或调试的场景。

#### 步骤 1：启动基础服务

```bash
# 终端 1: 启动 master
mooncake_master --default_kv_lease_ttl=5000
```

#### 步骤 2：运行 Embedded 模式

```bash
# 终端 2: Python benchmark
python3 client_mode_bench.py \
    --mode embedded \
    --protocol tcp \
    --metadata-server "http://127.0.0.1:8080/metadata" \
    --master-server "localhost:50051" \
    --global-segment-size 4096 \
    --local-buffer-size 512 \
    --value-sizes "1024,65536,1048576,4194304,16777216" \
    --num-ops 100 \
    --warmup-ops 10 \
    --batch-size 1 \
    --output-dir ./results/embedded

# 终端 2: C++ benchmark
./build/mooncake-store/benchmarks/client_mode_bench \
    --mode=embedded \
    --protocol=tcp \
    --metadata_connection_string="http://127.0.0.1:8080/metadata" \
    --master_address="localhost:50051" \
    --value_sizes="1024,65536,1048576,4194304,16777216" \
    --ops_per_thread=100 \
    --warmup_ops=10 \
    --output_json=./results/embedded/cpp_results.json
```

#### 步骤 3：重启 Master（清除状态）

```bash
# 停止并重启 master
kill %1
mooncake_master --default_kv_lease_ttl=5000 &
sleep 2
```

#### 步骤 4：启动 RealClient

```bash
# 终端 3: 启动独立的 RealClient 进程
mooncake_client \
    --protocol=tcp \
    --global_segment_size="4 GB" \
    --metadata_server="http://127.0.0.1:8080/metadata" \
    --master_server_address="127.0.0.1:50051" \
    --port=50052
```

#### 步骤 5：运行 Standalone 模式

```bash
# 终端 2: Python benchmark
python3 client_mode_bench.py \
    --mode standalone \
    --real-client-address "127.0.0.1:50052" \
    --global-segment-size 4096 \
    --local-buffer-size 512 \
    --value-sizes "1024,65536,1048576,4194304,16777216" \
    --num-ops 100 \
    --warmup-ops 10 \
    --batch-size 1 \
    --output-dir ./results/standalone

# 终端 2: C++ benchmark
./build/mooncake-store/benchmarks/client_mode_bench \
    --mode=standalone \
    --real_client_address="127.0.0.1:50052" \
    --value_sizes="1024,65536,1048576,4194304,16777216" \
    --ops_per_thread=100 \
    --warmup_ops=10 \
    --output_json=./results/standalone/cpp_results.json
```

#### 步骤 6：生成对比报告

```bash
python3 client_mode_bench.py --compare ./results/embedded ./results/standalone
```

---

## 多节点测试

适用于跨节点数据传输的真实性能评估。

### 集群拓扑

```
Node A (Master + Metadata)     Node B (Storage)           Node C (Client)
┌───────────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│ mooncake_master       │     │ mooncake_client   │     │ benchmark        │
│ (port 50051)          │     │ (RealClient)      │     │ (embedded 或     │
│                       │     │ (port 50052)      │     │  standalone)     │
│ HTTP metadata server  │     │                   │     │                  │
│ (port 8080)           │     │                   │     │                  │
└───────────────────────┘     └──────────────────┘     └──────────────────┘
```

### 步骤

#### 1. Node A: 启动 Master

```bash
mooncake_master --default_kv_lease_ttl=5000 --port=50051
```

#### 2. Node B: 启动 RealClient

```bash
# RDMA 场景
mooncake_client \
    --host=$(hostname -I | awk '{print $1}') \
    --protocol=rdma \
    --device_names="mlx5_0" \
    --global_segment_size="16 GB" \
    --metadata_server="http://<NodeA_IP>:8080/metadata" \
    --master_server_address="<NodeA_IP>:50051" \
    --port=50052
```

#### 3. Node C: 运行 Benchmark

**Embedded 模式**（Client 直接在 Node C 进程内运行）：
```bash
python3 client_mode_bench.py \
    --mode embedded \
    --protocol rdma \
    --device-name "mlx5_0" \
    --local-hostname "$(hostname -I | awk '{print $1}')" \
    --metadata-server "http://<NodeA_IP>:8080/metadata" \
    --master-server "<NodeA_IP>:50051" \
    --global-segment-size 4096 \
    --local-buffer-size 512 \
    --value-sizes "1048576,4194304,16777216" \
    --num-ops 200 \
    --warmup-ops 20 \
    --output-dir ./results/multinode_embedded
```

**Standalone 模式**（Client 连接 Node B 的 RealClient）：

需要先在 Node C 上启动一个本地 RealClient（或使用远程 RealClient）：
```bash
# Node C 上启动本地 RealClient
mooncake_client \
    --host=$(hostname -I | awk '{print $1}') \
    --protocol=rdma \
    --device_names="mlx5_0" \
    --global_segment_size="16 GB" \
    --metadata_server="http://<NodeA_IP>:8080/metadata" \
    --master_server_address="<NodeA_IP>:50051" \
    --port=50052

# 然后运行 standalone benchmark
python3 client_mode_bench.py \
    --mode standalone \
    --real-client-address "127.0.0.1:50052" \
    --global-segment-size 4096 \
    --local-buffer-size 512 \
    --value-sizes "1048576,4194304,16777216" \
    --num-ops 200 \
    --warmup-ops 20 \
    --output-dir ./results/multinode_standalone
```

#### 4. 对比

```bash
python3 client_mode_bench.py \
    --compare ./results/multinode_embedded ./results/multinode_standalone
```

---

## 参数调优建议

### Value Size 选择

| 场景 | 推荐 Value Size |
|------|----------------|
| 小 KV 对（元数据） | 1KB - 64KB |
| 标准 KVCache（单 token） | 1MB - 4MB |
| 大 KVCache（多 token batch） | 4MB - 16MB |
| 全面评估 | `1024,65536,1048576,4194304,16777216` |

### Batch Size 选择（Python benchmark）

| 场景 | 推荐 Batch Size |
|------|----------------|
| 延迟敏感（逐条操作） | 1 |
| 吞吐导向（decode 批量读取） | 4 - 16 |
| 压力测试 | 32 - 128 |

### 线程数

- 单线程测试反映最小延迟
- 多线程测试反映并发吞吐能力
- standalone 模式下 DummyClient 内部有锁，高并发场景可能出现竞争

### 内存配置

- `--global-segment-size`：embedded 模式下 Client 贡献给分布式池的内存大小
- `--local-buffer-size`：本地用于 Transfer Engine 的缓冲区大小
- 确保 `global-segment-size` 足以容纳所有 value size × num-ops 的数据量

---

## 输出文件说明

```
bench_results/
└── 20260225_143000/
    ├── embedded/
    │   ├── results.json          # Python benchmark 完整结果
    │   ├── results.csv           # Python benchmark 汇总（方便画图）
    │   ├── python_bench.log      # Python benchmark 日志
    │   ├── cpp_results.json      # C++ benchmark 结果
    │   └── cpp_bench.log         # C++ benchmark 日志
    └── standalone/
        ├── results.json
        ├── results.csv
        ├── python_bench.log
        ├── cpp_results.json
        └── cpp_bench.log
```

### JSON 格式（Python）

```json
[
  {
    "value_size": 1048576,
    "mode": "embedded",
    "put": {
      "total_ops": 100,
      "mean_ms": 1.23,
      "p90_ms": 1.45,
      "p99_ms": 2.10,
      "wall_throughput_mbps": 812.5,
      ...
    },
    "get": { ... }
  }
]
```

### CSV 格式

```csv
value_size,operation,total_ops,mean_ms,p90_ms,p99_ms,wall_throughput_mbps,...
1048576,put,100,1.23,1.45,2.10,812.5,...
1048576,get,100,0.89,1.02,1.55,1122.3,...
```

---

## 预期结果解读

根据 PR #1122 的 E2E 数据，embedded 和 standalone 模式的性能应非常接近（差异 <5%），因为 standalone 模式通过共享内存保持了零拷贝路径。

可能观察到的差异：
- **小 value size**（<64KB）：standalone 模式因 RPC 开销可能略慢（RPC round-trip 占比更大）
- **大 value size**（>1MB）：两种模式基本持平，数据传输时间主导
- **高并发**：standalone 模式可能因 DummyClient 内部序列化而略慢
- **首次 Get（冷数据）**：两种模式应基本一致（都走 Transfer Engine）
