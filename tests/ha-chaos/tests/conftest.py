"""
Mooncake HA Chaos Test — Shared Fixtures & Helpers

Provides:
  - MasterCluster: manage 3 master containers (kill/restart/pause/unpause)
  - ClientCluster: manage 2 real_client containers
  - StoreClient: DummyClient-based Python client for put/get/remove
  - MetricsChecker: query /metrics, /metrics/ha, /get_all_keys, /health
  - EtcdHelper: query etcd for master_view, leader detection
"""
import os
import time
import json
import hashlib
import subprocess
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

import pytest
import requests
from tenacity import retry, stop_after_delay, wait_fixed, retry_if_exception_type

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("chaos")

# ──────────────────────────── Configuration ──────────────────────────────────

ETCD_ENDPOINTS = os.getenv("ETCD_ENDPOINTS", "etcd:2379")
MASTER_HOSTS_STR = os.getenv("MASTER_HOSTS", "master-1:50051,master-2:50052,master-3:50053")
MASTER_HTTP_STR = os.getenv("MASTER_HTTP_PORTS", "master-1:9001,master-2:9002,master-3:9003")
CLIENT_ADDRS_STR = os.getenv("CLIENT_ADDRS", "client-1:12888,client-2:12889")
METADATA_SERVER = os.getenv("METADATA_SERVER", "http://http-meta:8080/metadata")

# Timeouts
LEADER_ELECTION_TIMEOUT = 45  # seconds to wait for new leader election
CLIENT_CRASH_DETECTION_TIMEOUT = 40  # seconds for master to detect client crash
HEALTH_CHECK_INTERVAL = 2
MASTER_VIEW_LEASE_TTL = 10  # etcd lease TTL for master view


@dataclass
class MasterInfo:
    name: str          # container name, e.g. "mc-master-1"
    rpc_host: str      # e.g. "master-1"
    rpc_port: int      # e.g. 50051
    http_host: str     # e.g. "master-1"
    http_port: int     # e.g. 9001

    @property
    def rpc_addr(self) -> str:
        return f"{self.rpc_host}:{self.rpc_port}"

    @property
    def http_url(self) -> str:
        return f"http://{self.http_host}:{self.http_port}"

    @property
    def container_name(self) -> str:
        return self.name


@dataclass
class ClientInfo:
    name: str       # container name, e.g. "mc-client-1"
    host: str       # e.g. "client-1"
    port: int       # e.g. 12888

    @property
    def rpc_addr(self) -> str:
        return f"{self.host}:{self.port}"

    @property
    def container_name(self) -> str:
        return self.name


def parse_masters() -> List[MasterInfo]:
    """Parse MASTER_HOSTS and MASTER_HTTP_PORTS env vars."""
    rpc_list = MASTER_HOSTS_STR.split(",")
    http_list = MASTER_HTTP_STR.split(",")
    masters = []
    for rpc_entry, http_entry in zip(rpc_list, http_list):
        rpc_host, rpc_port = rpc_entry.rsplit(":", 1)
        http_host, http_port = http_entry.rsplit(":", 1)
        idx = len(masters) + 1
        masters.append(MasterInfo(
            name=f"mc-master-{idx}",
            rpc_host=rpc_host,
            rpc_port=int(rpc_port),
            http_host=http_host,
            http_port=int(http_port),
        ))
    return masters


def parse_clients() -> List[ClientInfo]:
    """Parse CLIENT_ADDRS env var."""
    clients = []
    for entry in CLIENT_ADDRS_STR.split(","):
        host, port = entry.rsplit(":", 1)
        idx = len(clients) + 1
        clients.append(ClientInfo(
            name=f"mc-client-{idx}",
            host=host,
            port=int(port),
        ))
    return clients


# ──────────────────────────── Docker Control ─────────────────────────────────

class DockerCtl:
    """Control containers via docker CLI (works when /var/run/docker.sock is mounted)."""

    @staticmethod
    def _run(cmd: str, check: bool = True) -> str:
        result = subprocess.run(
            cmd, shell=True, capture_output=True, text=True, timeout=30
        )
        if check and result.returncode != 0:
            raise RuntimeError(f"docker cmd failed: {cmd}\nstderr: {result.stderr}")
        return result.stdout.strip()

    @classmethod
    def kill(cls, container: str) -> None:
        logger.warning(f"🔪 Killing container: {container}")
        cls._run(f"docker kill {container}", check=False)

    @classmethod
    def stop(cls, container: str, timeout: int = 5) -> None:
        logger.warning(f"⏹️  Stopping container: {container}")
        cls._run(f"docker stop -t {timeout} {container}", check=False)

    @classmethod
    def start(cls, container: str) -> None:
        logger.info(f"▶️  Starting container: {container}")
        cls._run(f"docker start {container}")

    @classmethod
    def restart(cls, container: str) -> None:
        logger.info(f"🔄 Restarting container: {container}")
        cls._run(f"docker restart {container}")

    @classmethod
    def pause(cls, container: str) -> None:
        logger.warning(f"⏸️  Pausing container: {container}")
        cls._run(f"docker pause {container}")

    @classmethod
    def unpause(cls, container: str) -> None:
        logger.info(f"▶️  Unpausing container: {container}")
        cls._run(f"docker unpause {container}")

    @classmethod
    def is_running(cls, container: str) -> bool:
        out = cls._run(
            f"docker inspect -f '{{{{.State.Running}}}}' {container}", check=False
        )
        return out.strip().lower() == "true"

    @classmethod
    def is_paused(cls, container: str) -> bool:
        out = cls._run(
            f"docker inspect -f '{{{{.State.Paused}}}}' {container}", check=False
        )
        return out.strip().lower() == "true"


# ──────────────────────────── Etcd Helper ────────────────────────────────────

class EtcdHelper:
    """Query etcd for master view (leader election state)."""

    def __init__(self, endpoints: str = ETCD_ENDPOINTS):
        self.endpoints = endpoints

    def _etcdctl(self, *args) -> str:
        cmd = ["etcdctl", f"--endpoints=http://{self.endpoints}"] + list(args)
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
        return result.stdout.strip()

    def get_master_view(self) -> Optional[str]:
        """Get current leader address from etcd master_view key."""
        try:
            output = self._etcdctl("get", "mooncake/master_view", "--print-value-only")
            if output:
                # The value may be JSON or plain address
                try:
                    data = json.loads(output)
                    return data.get("address", output)
                except json.JSONDecodeError:
                    return output
        except Exception as e:
            logger.debug(f"etcdctl get master_view failed: {e}")
        return None

    def get_all_keys_with_prefix(self, prefix: str) -> Dict[str, str]:
        """Get all etcd keys with a given prefix."""
        try:
            output = self._etcdctl("get", prefix, "--prefix")
            result = {}
            lines = output.split("\n")
            for i in range(0, len(lines) - 1, 2):
                result[lines[i]] = lines[i + 1]
            return result
        except Exception:
            return {}


# ──────────────────────────── Metrics Checker ────────────────────────────────

class MetricsChecker:
    """Query master HTTP endpoints for metrics, keys, and health."""

    def __init__(self, master: MasterInfo):
        self.master = master

    def is_healthy(self) -> bool:
        try:
            r = requests.get(f"{self.master.http_url}/health", timeout=3)
            return r.status_code == 200
        except Exception:
            return False

    def get_metrics(self) -> str:
        """Get Prometheus-format metrics text."""
        r = requests.get(f"{self.master.http_url}/metrics", timeout=5)
        r.raise_for_status()
        return r.text

    def get_ha_metrics(self) -> str:
        """Get HA-specific Prometheus metrics."""
        r = requests.get(f"{self.master.http_url}/metrics/ha", timeout=5)
        r.raise_for_status()
        return r.text

    def get_metrics_summary(self) -> str:
        """Get human-readable metrics summary."""
        r = requests.get(f"{self.master.http_url}/metrics/summary", timeout=5)
        r.raise_for_status()
        return r.text

    def get_all_keys(self) -> Set[str]:
        """Get all keys stored in this master."""
        r = requests.get(f"{self.master.http_url}/get_all_keys", timeout=10)
        r.raise_for_status()
        keys = set()
        for line in r.text.strip().split("\n"):
            line = line.strip()
            if line:
                keys.add(line)
        return keys

    def get_all_segments(self) -> Set[str]:
        """Get all segments registered on this master."""
        r = requests.get(f"{self.master.http_url}/get_all_segments", timeout=5)
        r.raise_for_status()
        segments = set()
        for line in r.text.strip().split("\n"):
            line = line.strip()
            if line:
                segments.add(line)
        return segments

    def parse_metric_value(self, metrics_text: str, metric_name: str) -> Optional[float]:
        """Parse a specific metric value from Prometheus text format."""
        for line in metrics_text.split("\n"):
            line = line.strip()
            if line.startswith("#"):
                continue
            if metric_name in line:
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        return float(parts[-1])
                    except ValueError:
                        continue
        return None

    def get_key_count(self) -> int:
        metrics = self.get_metrics()
        val = self.parse_metric_value(metrics, "master_key_count")
        return int(val) if val is not None else -1

    def get_active_clients(self) -> int:
        metrics = self.get_metrics()
        val = self.parse_metric_value(metrics, "master_active_clients")
        return int(val) if val is not None else -1

    def get_oplog_last_seq(self) -> int:
        metrics = self.get_ha_metrics()
        val = self.parse_metric_value(metrics, "ha_oplog_last_sequence_id")
        return int(val) if val is not None else -1

    def get_oplog_standby_lag(self) -> int:
        metrics = self.get_ha_metrics()
        val = self.parse_metric_value(metrics, "ha_oplog_standby_lag")
        return int(val) if val is not None else -1

    def get_oplog_checksum_failures(self) -> int:
        metrics = self.get_ha_metrics()
        val = self.parse_metric_value(metrics, "ha_oplog_checksum_failures_total")
        return int(val) if val is not None else 0


# ──────────────────────────── Store Client (Python) ──────────────────────────

class StoreClient:
    """
    A Python wrapper around MooncakeDistributedStore in DummyClient mode.
    Connects to one of the real_client containers over coro_rpc.

    Usage:
        client = StoreClient("client-1:12888")
        client.setup()
        client.put("key1", b"hello world")
        data = client.get("key1")
        assert data == b"hello world"
    """

    def __init__(self, server_address: str,
                 mem_pool_size: int = 512 * 1024 * 1024,
                 local_buffer_size: int = 128 * 1024 * 1024):
        self.server_address = server_address
        self.mem_pool_size = mem_pool_size
        self.local_buffer_size = local_buffer_size
        self.store = None
        self._ready = False

    @retry(stop=stop_after_delay(60), wait=wait_fixed(3),
           retry=retry_if_exception_type(Exception))
    def setup(self):
        """Initialize the DummyClient connection (with retry)."""
        from mooncake.store import MooncakeDistributedStore
        self.store = MooncakeDistributedStore()
        retcode = self.store.setup_dummy(
            self.mem_pool_size,
            self.local_buffer_size,
            self.server_address,
        )
        if retcode != 0:
            raise RuntimeError(f"setup_dummy failed with code {retcode}")
        self._ready = True
        logger.info(f"StoreClient connected to {self.server_address}")

    def put(self, key: str, value: bytes) -> int:
        assert self._ready, "Client not initialized"
        return self.store.put(key, value)

    def get(self, key: str) -> bytes:
        assert self._ready, "Client not initialized"
        return self.store.get(key)

    def remove(self, key: str) -> int:
        assert self._ready, "Client not initialized"
        return self.store.remove(key)

    def is_exist(self, key: str) -> bool:
        assert self._ready, "Client not initialized"
        return self.store.is_exist(key) == 1

    def get_size(self, key: str) -> int:
        assert self._ready, "Client not initialized"
        return self.store.get_size(key)

    def close(self):
        self.store = None
        self._ready = False


# ──────────────────────────── Data Integrity ─────────────────────────────────

class DataIntegrityTracker:
    """
    Tracks what data has been successfully written, so we can verify after chaos.

    For each key, stores the sha256 digest and size of the value.
    After failover, reads back every key and verifies the data matches.
    """

    def __init__(self):
        self.records: Dict[str, Tuple[str, int]] = {}  # key -> (sha256_hex, size)

    def record_put(self, key: str, value: bytes):
        digest = hashlib.sha256(value).hexdigest()
        self.records[key] = (digest, len(value))

    def record_remove(self, key: str):
        self.records.pop(key, None)

    @property
    def keys(self) -> Set[str]:
        return set(self.records.keys())

    @property
    def key_count(self) -> int:
        return len(self.records)

    def verify_all(self, client: StoreClient) -> List[str]:
        """
        Verify every tracked key matches expected data.
        Returns list of error messages (empty = all good).
        """
        errors = []
        for key, (expected_hash, expected_size) in self.records.items():
            try:
                data = client.get(key)
                if data == b"":
                    errors.append(f"Key '{key}': expected {expected_size}B but got empty")
                    continue
                actual_hash = hashlib.sha256(data).hexdigest()
                if actual_hash != expected_hash:
                    errors.append(
                        f"Key '{key}': hash mismatch! "
                        f"expected={expected_hash[:16]}... got={actual_hash[:16]}..."
                    )
                if len(data) != expected_size:
                    errors.append(
                        f"Key '{key}': size mismatch! "
                        f"expected={expected_size} got={len(data)}"
                    )
            except Exception as e:
                errors.append(f"Key '{key}': get failed with {e}")
        return errors

    def verify_keys_on_master(self, checker: MetricsChecker) -> List[str]:
        """Verify that the master's key set matches our tracker."""
        errors = []
        try:
            master_keys = checker.get_all_keys()
            expected_keys = self.keys
            missing = expected_keys - master_keys
            extra = master_keys - expected_keys
            if missing:
                errors.append(f"Keys missing from master: {missing}")
            if extra:
                # Extra keys might be from segments, ignore non-tracked keys
                logger.debug(f"Extra keys on master (may be segment keys): {extra}")
        except Exception as e:
            errors.append(f"Failed to get keys from master: {e}")
        return errors


# ──────────────────────────── Leader Finder ──────────────────────────────────

def find_leader(masters: List[MasterInfo]) -> Optional[MasterInfo]:
    """Find the current leader master by checking /health on each."""
    for m in masters:
        try:
            checker = MetricsChecker(m)
            if checker.is_healthy():
                # Healthy master responding = likely the leader
                # Double-check by getting key count (only leader can serve)
                try:
                    checker.get_all_keys()
                    return m
                except Exception:
                    continue
        except Exception:
            continue
    return None


@retry(stop=stop_after_delay(LEADER_ELECTION_TIMEOUT), wait=wait_fixed(HEALTH_CHECK_INTERVAL),
       retry=retry_if_exception_type(Exception))
def wait_for_leader(masters: List[MasterInfo]) -> MasterInfo:
    """Wait until a leader is elected and healthy."""
    leader = find_leader(masters)
    if leader is None:
        raise RuntimeError("No leader found yet")
    logger.info(f"✅ Leader found: {leader.name} ({leader.rpc_addr})")
    return leader


@retry(stop=stop_after_delay(60), wait=wait_fixed(3),
       retry=retry_if_exception_type(Exception))
def wait_for_client_healthy(client_info: ClientInfo) -> bool:
    """Wait for a real_client container to be responding."""
    if not DockerCtl.is_running(client_info.container_name):
        raise RuntimeError(f"Client {client_info.name} not running")
    # We can't HTTP-check the client directly (no HTTP server by default),
    # so just verify the container is up
    return True


def wait_for_standby_sync(masters: List[MasterInfo], timeout: int = 60) -> None:
    """Wait until all healthy standbys have caught up (lag=0)."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        all_synced = True
        for m in masters:
            try:
                checker = MetricsChecker(m)
                if not checker.is_healthy():
                    continue
                lag = checker.get_oplog_standby_lag()
                if lag > 0:
                    all_synced = False
                    logger.debug(f"  {m.name} standby lag: {lag}")
            except Exception:
                continue
        if all_synced:
            logger.info("✅ All standbys synced (lag=0)")
            return
        time.sleep(2)
    logger.warning("⚠️  Standby sync timeout reached")


# ──────────────────────────── Pytest Fixtures ────────────────────────────────

@pytest.fixture(scope="session")
def masters() -> List[MasterInfo]:
    return parse_masters()


@pytest.fixture(scope="session")
def clients() -> List[ClientInfo]:
    return parse_clients()


@pytest.fixture(scope="session")
def etcd() -> EtcdHelper:
    return EtcdHelper()


@pytest.fixture(scope="function")
def tracker() -> DataIntegrityTracker:
    return DataIntegrityTracker()


@pytest.fixture(scope="function")
def ensure_cluster_ready(masters, clients):
    """Ensure all masters and clients are running before each test."""
    # Start all masters
    for m in masters:
        if not DockerCtl.is_running(m.container_name):
            DockerCtl.start(m.container_name)
        if DockerCtl.is_paused(m.container_name):
            DockerCtl.unpause(m.container_name)

    # Start all clients
    for c in clients:
        if not DockerCtl.is_running(c.container_name):
            DockerCtl.start(c.container_name)
        if DockerCtl.is_paused(c.container_name):
            DockerCtl.unpause(c.container_name)

    # Wait for leader election
    leader = wait_for_leader(masters)
    logger.info(f"Cluster ready. Leader: {leader.name}")

    # Wait for clients
    for c in clients:
        wait_for_client_healthy(c)

    # Small grace period for everything to stabilize
    time.sleep(5)

    yield leader

    # Teardown: make sure everything is restarted for next test
    for m in masters:
        if DockerCtl.is_paused(m.container_name):
            DockerCtl.unpause(m.container_name)
        if not DockerCtl.is_running(m.container_name):
            DockerCtl.start(m.container_name)
    for c in clients:
        if DockerCtl.is_paused(c.container_name):
            DockerCtl.unpause(c.container_name)
        if not DockerCtl.is_running(c.container_name):
            DockerCtl.start(c.container_name)
    # Wait for stabilization
    time.sleep(10)
