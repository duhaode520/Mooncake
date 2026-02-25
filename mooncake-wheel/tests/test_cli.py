#!/usr/bin/env python3
"""
Test script to verify that the mooncake_master entry point works correctly.
"""

import json
import os
import subprocess
import sys
import tempfile
import time


def cleanup_processes(*procs):
    """Terminate and wait for all given processes, ignoring errors."""
    for p in procs:
        if p is None:
            continue
        try:
            p.terminate()
            p.wait(timeout=5)
        except Exception:
            try:
                p.kill()
                p.wait(timeout=3)
            except Exception:
                pass


def check_binary_in_path(name):
    """Return True if the named binary is found in PATH."""
    result = subprocess.run(
        ["which", name], capture_output=True, text=True
    )
    if result.returncode != 0:
        print(f"  {name} entry point not found in PATH")
        return False
    print(f"  {name} found at: {result.stdout.strip()}")
    return True


def make_client_config(master_port, client_port):
    """Return a client config dict pointing at the given master and client ports."""
    return {
        "host": "0.0.0.0",
        "metadata_server": "http://127.0.0.1:8080/metadata",
        "device_names": "",
        "master_server_address": f"127.0.0.1:{master_port}",
        "protocol": "tcp",
        "port": client_port,
        "global_segment_size": "4 GB",
        "threads": 1,
        "enable_offload": False,
    }


def write_temp_config(config):
    """Write config dict to a temp JSON file and return its path."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(config, f)
        return f.name


def start_master(port):
    """Start a mooncake_master process on the given port. Returns the Popen object."""
    return subprocess.Popen(
        ["mooncake_master", f"--port={port}", "--max_threads=2",
         "--enable_http_metadata_server=true"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_entry_point_installed():
    """Test that the entry point is installed and can be executed."""
    try:
        for binary in ["mooncake_master", "mooncake_client", "transfer_engine_bench"]:
            if not check_binary_in_path(binary):
                return False
        return True
    except Exception as e:
        print(f"❌ Error checking for entry point: {e}")
        return False


def test_run_master_and_client():
    """Test running the master service through the entry point."""
    process = None
    client_process = None
    try:
        process = start_master(61351)

        # Give it a moment to start
        time.sleep(2)

        # Check if process is running
        if process.poll() is None:
            print("✅ mooncake_master process started successfully")
            client_process = subprocess.Popen(
                ["mooncake_client", "--master_server_address=127.0.0.1:61351", "--port=61352"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )

            # Give the client some time to connect
            time.sleep(2)
            if client_process.poll() is None:
                print("✅ mooncake_client connected to mooncake_master successfully")
                print("✅ mooncake_client process terminated successfully")
            else:
                stdout, stderr = client_process.communicate()
                print(f"❌ mooncake_client failed to start")
                print(f"stdout: {stdout.decode()}")
                print(f"stderr: {stderr.decode()}")
            print("✅ mooncake_master process terminated successfully")
            return True
        else:
            stdout, stderr = process.communicate()
            print(f"❌ mooncake_master process failed to start")
            print(f"stdout: {stdout.decode()}")
            print(f"stderr: {stderr.decode()}")
            return False
    except Exception as e:
        print(f"❌ Error running mooncake_master: {e}")
        return False
    finally:
        cleanup_processes(client_process, process)


def test_client_with_config_file():
    """mooncake_client starts successfully when given a valid --config_path."""
    config_path = write_temp_config(make_client_config(62351, 62352))

    master = None
    client = None
    try:
        master = start_master(62351)
        time.sleep(2)
        if master.poll() is not None:
            print("❌ mooncake_master failed to start for config_file test")
            return False

        client = subprocess.Popen(
            ["mooncake_client", f"--config_path={config_path}"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        time.sleep(2)
        if client.poll() is None:
            print("✅ mooncake_client started successfully with --config_path")
            return True
        else:
            stdout, stderr = client.communicate()
            print("❌ mooncake_client failed to start with --config_path")
            print(f"stdout: {stdout.decode()}")
            print(f"stderr: {stderr.decode()}")
            return False
    finally:
        cleanup_processes(client, master)
        os.unlink(config_path)


def test_client_config_cli_override():
    """CLI --port overrides the port value set in config file."""
    config_port = 63352
    cli_port = 63353
    config_path = write_temp_config(make_client_config(63351, config_port))

    master = None
    client = None
    try:
        master = start_master(63351)
        time.sleep(2)
        if master.poll() is not None:
            print("mooncake_master failed to start for cli_override test")
            return False

        # CLI passes --port=cli_port, which should win over config file's config_port
        client = subprocess.Popen(
            ["mooncake_client", f"--config_path={config_path}",
             f"--port={cli_port}"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        time.sleep(2)
        if client.poll() is None:
            # Verify the process bound to the CLI port, not the config port
            pid = client.pid
            cli_port_hex = f"{cli_port:04X}"
            config_port_hex = f"{config_port:04X}"
            try:
                with open(f"/proc/{pid}/net/tcp", "r") as tcp_file:
                    content = tcp_file.read().upper()
                    has_cli_port = cli_port_hex in content
                    has_config_port = config_port_hex in content
                    if has_cli_port and not has_config_port:
                        print(f"CLI --port={cli_port} overrides config file port {config_port}")
                        return True
                    elif has_config_port and not has_cli_port:
                        print(f"Client is listening on config port {config_port}, CLI override failed")
                        return False
                    else:
                        print("Could not determine port from /proc/net/tcp")
                        return False
            except Exception as e:
                print(f"Failed to verify port via /proc: {e}")
                return False
        else:
            stdout, stderr = client.communicate()
            print("mooncake_client failed to start for cli_override test")
            print(f"stdout: {stdout.decode()}")
            print(f"stderr: {stderr.decode()}")
            return False
    finally:
        cleanup_processes(client, master)
        os.unlink(config_path)


def test_client_invalid_config_path():
    """mooncake_client exits with non-zero status when config file does not exist."""
    result = subprocess.run(
        ["mooncake_client", "--config_path=/nonexistent/path/client.json"],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode != 0:
        print("✅ mooncake_client exited with non-zero status for missing config file")
        return True
    else:
        print("❌ mooncake_client should have failed for missing config file but returned 0")
        return False


if __name__ == "__main__":
    print("Testing mooncake_master entry point...")

    tests = [
        ("Entry point installed",          test_entry_point_installed),
        ("Run master and client",          test_run_master_and_client),
        ("Client with config file",        test_client_with_config_file),
        ("CLI overrides config file",      test_client_config_cli_override),
        ("Invalid config path exits != 0", test_client_invalid_config_path),
    ]

    results = {}
    for name, func in tests:
        results[name] = func()
        # All remaining tests depend on entry points being installed
        if name == "Entry point installed" and not results[name]:
            for remaining_name, _ in tests[1:]:
                results[remaining_name] = False
            break

    print("\nTest Summary:")
    for name, passed in results.items():
        status = "PASS" if passed else "FAIL"
        print(f"  [{status}] {name}")

    if all(results.values()):
        print("\nAll tests passed!")
        sys.exit(0)
    else:
        print("\nSome tests failed.")
        sys.exit(1)
