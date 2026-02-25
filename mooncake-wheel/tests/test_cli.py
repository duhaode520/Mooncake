#!/usr/bin/env python3
"""
Test script to verify that the mooncake_master entry point works correctly.
"""

import glob
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


def test_entry_point_installed():
    """Test that the entry point is installed and can be executed."""
    try:
        # Check if mooncake_master is in PATH
        result = subprocess.run(
            ["which", "mooncake_master"],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            print("❌ mooncake_master entry point not found in PATH")
            return False

        print(f"✅ mooncake_master entry point found at: {result.stdout.strip()}")
        result = subprocess.run(
            ["which", "mooncake_client"],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            print("❌ mooncake_client entry point not found in PATH")
            return False

        print(f"✅ mooncake_client entry point found at: {result.stdout.strip()}")
        result = subprocess.run(
            ["which", "transfer_engine_bench"],
            capture_output=True,
            text=True
        )

        if result.returncode != 0:
            print("❌ transfer_engine_bench entry point not found in PATH")
            return False

        print(f"✅ transfer_engine_bench entry point found at: {result.stdout.strip()}")
        return True
    except Exception as e:
        print(f"❌ Error checking for entry point: {e}")
        return False


def test_run_master_and_client():
    """Test running the master service through the entry point."""
    process = None
    client_process = None
    try:
        # Run mooncake_master with a non-default port to avoid conflicts
        process = subprocess.Popen(
            ["mooncake_master", "--port=61351", "--max_threads=2", "--enable_http_metadata_server=true"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )

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
    config = {
        "host": "0.0.0.0",
        "metadata_server": "http://127.0.0.1:8080/metadata",
        "device_names": "",
        "master_server_address": "127.0.0.1:62351",
        "protocol": "tcp",
        "port": 62352,
        "global_segment_size": "4 GB",
        "threads": 1,
        "enable_offload": False,
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(config, f)
        config_path = f.name

    master = None
    client = None
    try:
        master = subprocess.Popen(
            ["mooncake_master", "--port=62351", "--max_threads=2",
             "--enable_http_metadata_server=true"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
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
    config = {
        "host": "0.0.0.0",
        "metadata_server": "http://127.0.0.1:8080/metadata",
        "device_names": "",
        "master_server_address": "127.0.0.1:63351",
        "protocol": "tcp",
        "port": 63352,        # config file says 63352
        "global_segment_size": "4 GB",
        "threads": 1,
        "enable_offload": False,
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(config, f)
        config_path = f.name

    master = None
    client = None
    try:
        master = subprocess.Popen(
            ["mooncake_master", "--port=63351", "--max_threads=2",
             "--enable_http_metadata_server=true"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        time.sleep(2)
        if master.poll() is not None:
            print("❌ mooncake_master failed to start for cli_override test")
            return False

        # CLI passes --port=63353, which should win over config file's 63352
        client = subprocess.Popen(
            ["mooncake_client", f"--config_path={config_path}", "--port=63353"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        time.sleep(2)
        if client.poll() is None:
            # Verify the process bound to CLI port (63353) not config port (63352)
            pid = client.pid
            # 63353 in hex is F799, 63352 in hex is F798
            try:
                with open(f"/proc/{pid}/net/tcp", "r") as tcp_file:
                    content = tcp_file.read().upper()
                    has_cli_port = "F799" in content
                    has_config_port = "F798" in content
                    if has_cli_port and not has_config_port:
                        print("✅ CLI --port=63353 overrides config file port 63352")
                        return True
                    elif has_config_port and not has_cli_port:
                        print("❌ Client is listening on config port 63352, CLI override failed")
                        return False
                    else:
                        print("❌ Could not determine port from /proc/net/tcp")
                        return False
            except Exception as e:
                print(f"❌ Failed to verify port via /proc: {e}")
                return False
        else:
            stdout, stderr = client.communicate()
            print("❌ mooncake_client failed to start for cli_override test")
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

    entry_point_installed = test_entry_point_installed()

    if entry_point_installed:
        run_master_and_client_success = test_run_master_and_client()
        config_file_success = test_client_with_config_file()
        cli_override_success = test_client_config_cli_override()
        invalid_config_success = test_client_invalid_config_path()
    else:
        run_master_and_client_success = False
        config_file_success = False
        cli_override_success = False
        invalid_config_success = False

    print("\nTest Summary:")
    print(f"Entry point installed:          {'✅' if entry_point_installed else '❌'}")
    print(f"Run master and client:          {'✅' if run_master_and_client_success else '❌'}")
    print(f"Client with config file:        {'✅' if config_file_success else '❌'}")
    print(f"CLI overrides config file:      {'✅' if cli_override_success else '❌'}")
    print(f"Invalid config path exits != 0: {'✅' if invalid_config_success else '❌'}")

    if all([entry_point_installed, run_master_and_client_success,
            config_file_success, cli_override_success, invalid_config_success]):
        print("\nAll tests passed! 🎉")
        sys.exit(0)
    else:
        print("\nSome tests failed. 😢")
        sys.exit(1)
