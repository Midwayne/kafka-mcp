#!/usr/bin/env python3
"""Build versioned binaries, desktop bundles, a launcher package, and checksums."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
MCPB_VERSION = "2.1.2"
TARGETS = [(system, arch) for system in ("linux", "darwin", "windows")
           for arch in ("amd64", "arm64")]


def run(*command, **options):
    subprocess.run(command, cwd=ROOT, check=True, **options)


def manifest(version, system, executable):
    return {
        "manifest_version": "0.3", "name": "kafka-mcp",
        "display_name": "Kafka MCP", "version": version,
        "description": "Explore, search, and aggregate your Kafka clusters with an AI agent.",
        "author": {"name": "Midwayne", "url": "https://github.com/Midwayne"},
        "homepage": "https://github.com/Midwayne/kafka-mcp",
        "documentation": "https://github.com/Midwayne/kafka-mcp#quick-start",
        "repository": {"type": "git", "url": "https://github.com/Midwayne/kafka-mcp.git"},
        "server": {
            "type": "binary", "entry_point": f"server/{executable}",
            "mcp_config": {
                "command": "${__dirname}/server/" + executable,
                "args": ["-config", "${user_config.config_path}"],
            },
        },
        "compatibility": {"platforms": ["win32" if system == "windows" else system]},
        "tools_generated": True,
        "keywords": ["kafka", "messages", "search", "streaming"],
        "user_config": {
            "config_path": {
                "type": "file", "title": "Kafka configuration",
                "description": "Choose your kafka-mcp YAML or JSON file with cluster connections and authentication.",
                "required": True,
            },
        },
    }


def build_binary(version, system, arch, output):
    name = f"kafka-mcp_{version}_{system}_{arch}"
    if system == "windows":
        name += ".exe"
    environment = dict(os.environ, GOOS=system, GOARCH=arch, CGO_ENABLED="0")
    run("go", "build", "-mod=readonly", "-trimpath", "-buildvcs=false",
        "-ldflags", f"-s -w -buildid= -X main.version={version}",
        "-o", str(output / name), ".", env=environment)
    return output / name


def build_bundle(version, system, arch, binary, output, staging):
    bundle = staging / f"{system}-{arch}"
    (bundle / "server").mkdir(parents=True)
    executable = "kafka-mcp.exe" if system == "windows" else "kafka-mcp"
    shutil.copy2(binary, bundle / "server" / executable)
    shutil.copy2(ROOT / "README.md", bundle / "README.md")
    shutil.copy2(ROOT / "examples" / "minimal.yaml", bundle / "config.example.yaml")
    metadata = manifest(version, system, executable)
    (bundle / "manifest.json").write_text(json.dumps(metadata, indent=2) + "\n")
    destination = output / f"kafka-mcp_{version}_{system}_{arch}.mcpb"
    run("npx", "--yes", f"@anthropic-ai/mcpb@{MCPB_VERSION}", "pack", str(bundle), str(destination))


def notes(version, destination):
    changelog = (ROOT / "CHANGELOG.md").read_text()
    match = re.search(r"^## \[" + re.escape(version) + r"\].*\n(.*?)(?=^## |\Z)",
                      changelog, flags=re.MULTILINE | re.DOTALL)
    if not match:
        raise SystemExit(f"CHANGELOG.md has no entry for {version}")
    destination.write_text(match[1].strip() + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("version", help="SemVer, with or without a leading v")
    parser.add_argument("--output", type=Path, default=ROOT / "dist")
    parser.add_argument("--notes", type=Path)
    args = parser.parse_args()
    version = args.version.removeprefix("v")
    if not re.fullmatch(r"\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?", version):
        raise SystemExit("Version must be SemVer, such as 1.0.0 or 1.1.0-rc.1")
    package = json.loads((ROOT / "package.json").read_text())
    if version != package["version"]:
        raise SystemExit("Release tag and package.json version must match")
    output = args.output.resolve()
    if output.exists() and any(output.iterdir()):
        raise SystemExit(f"Refusing to mix release assets: {output} is not empty")
    output.mkdir(parents=True, exist_ok=True)
    if args.notes:
        notes(version, args.notes)

    with tempfile.TemporaryDirectory(prefix="kafka-mcp-release-") as directory:
        staging = Path(directory)
        for system, arch in TARGETS:
            print(f"Building {system}/{arch}", flush=True)
            binary = build_binary(version, system, arch, output)
            if system != "linux":
                build_bundle(version, system, arch, binary, output, staging)
        packed = subprocess.check_output(
            ["npm", "pack", "--json", "--pack-destination", str(staging)], cwd=ROOT)
        filename = json.loads(packed)[0]["filename"]
        shutil.move(str(staging / filename), output / f"kafka-mcp-{version}.tgz")

    lines = []
    for asset in sorted(output.iterdir()):
        with asset.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
        lines.append(f"{digest}  {asset.name}\n")
    (output / "SHA256SUMS").write_text("".join(lines))
    print(f"Release assets ready in {output}", flush=True)


if __name__ == "__main__":
    main()
