#!/usr/bin/env python3
"""Verify a released binary's version and stdio MCP handshake without Kafka."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import queue
import shutil
import subprocess
import tarfile
import tempfile
import threading


def exchange(process, responses, request):
    process.stdin.write(json.dumps(request) + "\n")
    process.stdin.flush()
    while True:
        line = responses.get(timeout=20)
        if not line:
            raise RuntimeError("MCP server exited before responding")
        response = json.loads(line)
        if response.get("id") == request["id"]:
            if "error" in response:
                raise RuntimeError(str(response["error"]))
            return response["result"]


def smoke(command, version, environment=None):
    actual = subprocess.check_output(command + ["-version"], text=True, env=environment).strip()
    if actual != version:
        raise RuntimeError(f"Version mismatch: {actual!r} != {version!r}")
    with tempfile.TemporaryDirectory(prefix="kafka-mcp-smoke-") as directory:
        config = Path(directory) / "config.json"
        config.write_text(json.dumps({"server": {"state_dir": str(Path(directory) / "state")},
                                      "clusters": [{"name": "smoke", "brokers": ["localhost:9092"]}]}))
        process = subprocess.Popen(command + ["-config", str(config)], stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE, text=True, env=environment)
        responses = queue.Queue()

        def read():
            for line in process.stdout:
                responses.put(line)
            responses.put("")

        threading.Thread(target=read, daemon=True).start()
        try:
            result = exchange(process, responses, {"jsonrpc": "2.0", "id": 1, "method": "initialize",
                "params": {"protocolVersion": "2025-03-26", "capabilities": {},
                           "clientInfo": {"name": "release-smoke", "version": "1.0.0"}}})
            if result["serverInfo"]["version"] != version:
                raise RuntimeError("MCP serverInfo does not match the release version")
            process.stdin.write('{"jsonrpc":"2.0","method":"notifications/initialized"}\n')
            process.stdin.flush()
            result = exchange(process, responses, {"jsonrpc": "2.0", "id": 2, "method": "tools/list"})
            names = {tool["name"] for tool in result["tools"]}
            if not {"list_clusters", "scan_messages", "count_messages"} <= names or "produce_message" in names:
                raise RuntimeError(f"Unexpected read-only tool catalog: {names}")
            result = exchange(process, responses, {"jsonrpc": "2.0", "id": 3, "method": "tools/call",
                              "params": {"name": "list_clusters", "arguments": {}}})
            clusters = json.loads(next(item["text"] for item in result.get("content", [])
                                       if item.get("type") == "text"))
            if result.get("isError") or [cluster["name"] for cluster in clusters] != ["smoke"]:
                raise RuntimeError(f"Cannot list configured clusters: {result}")
        finally:
            process.stdin.close()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
    print("Version, MCP initialization, tool discovery, and cluster discovery passed.")


def smoke_launcher(distribution, binary, version, system, arch):
    with tempfile.TemporaryDirectory(prefix="kafka-mcp-launcher-smoke-") as directory:
        root = Path(directory)
        with tarfile.open(distribution / f"kafka-mcp-{version}.tgz") as archive:
            archive.extractall(root, filter="data")
        node_platform = "win32" if system == "windows" else system
        node_arch = "x64" if arch == "amd64" else arch
        cache = root / "cache"
        cached = cache / version / f"{node_platform}-{node_arch}" / binary.name
        cached.parent.mkdir(parents=True)
        shutil.copy2(binary, cached)
        with binary.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
        cached.with_name(cached.name + ".sha256").write_text(digest)
        environment = dict(os.environ, KAFKA_MCP_CACHE_DIR=str(cache))
        launcher = root / "package/npm/bin/kafka-mcp.cjs"
        print("Checking the packaged launcher against the native binary...")
        smoke(["node", str(launcher)], version, environment)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True)
    parser.add_argument("--native-dist", type=Path)
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    command = args.command
    if command[:1] == ["--"]:
        command = command[1:]
    if args.native_dist:
        system = {"Darwin": "darwin", "Linux": "linux", "Windows": "windows"}[platform.system()]
        arch = {"x86_64": "amd64", "AMD64": "amd64", "arm64": "arm64", "aarch64": "arm64"}[platform.machine()]
        suffix = ".exe" if system == "windows" else ""
        binary = args.native_dist / f"kafka-mcp_{args.version}_{system}_{arch}{suffix}"
        if os.name != "nt":
            binary.chmod(0o700)
        command = [str(binary.resolve())]
    if not command:
        parser.error("provide --native-dist or a command after --")
    smoke(command, args.version)
    if args.native_dist:
        smoke_launcher(args.native_dist, binary, args.version, system, arch)


if __name__ == "__main__":
    main()
