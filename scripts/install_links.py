#!/usr/bin/env python3
"""Generate version-pinned client install links in README.md."""

import argparse
import base64
import json
from pathlib import Path
from urllib.parse import quote, urlencode

ROOT = Path(__file__).resolve().parents[1]
START = "<!-- install-links:start -->"
END = "<!-- install-links:end -->"


def content():
    version = json.loads((ROOT / "package.json").read_text())["version"]
    release = f"https://github.com/Midwayne/kafka-mcp/releases/download/v{version}"
    package = f"{release}/kafka-mcp-{version}.tgz"
    config = {"command": "npx", "args": ["--yes", f"--package={package}", "kafka-mcp"]}
    encoded = base64.b64encode(json.dumps(config, separators=(",", ":")).encode()).decode()
    cursor = "https://cursor.com/en/install-mcp?" + urlencode({"name": "kafka", "config": encoded})
    vscode_config = dict(name="kafka", **config)
    deep_link = "vscode:mcp/install?" + quote(json.dumps(vscode_config, separators=(",", ":")), safe="")
    vscode = "https://insiders.vscode.dev/redirect?" + urlencode({"url": deep_link})
    lines = [START,
        f"[![Install in Cursor](https://cursor.com/deeplink/mcp-install-dark.svg)]({cursor})",
        f"[![Install in VS Code](https://img.shields.io/badge/Install_in-VS_Code-007ACC)]({vscode})",
        "", "**Claude Desktop:** download and open the bundle for your computer, then select your Kafka configuration file.",
        "", "| macOS | Windows |", "|---|---|",
        f"| [Apple Silicon]({release}/kafka-mcp_{version}_darwin_arm64.mcpb) | [Intel / AMD]({release}/kafka-mcp_{version}_windows_amd64.mcpb) |",
        f"| [Intel]({release}/kafka-mcp_{version}_darwin_amd64.mcpb) | [ARM64]({release}/kafka-mcp_{version}_windows_arm64.mcpb) |",
        END]
    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    readme = ROOT / "README.md"
    original = readme.read_text()
    start, end = original.index(START), original.index(END) + len(END)
    updated = original[:start] + content() + original[end:]
    if args.check:
        if original != updated:
            raise SystemExit("Install links are stale; run python3 scripts/install_links.py")
    else:
        readme.write_text(updated)


if __name__ == "__main__":
    main()
