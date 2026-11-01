# Release strategy

Kafka MCP is distributed as a local MCP server. Releases publish installable binaries and desktop bundles on GitHub; operators can also run the same binary as an HTTP service using their own infrastructure and configuration.

## Versions and compatibility

Use Semantic Versioning with immutable annotated tags: `v1.0.0`, `v1.0.1`, `v1.1.0`. Candidate releases use tags such as `v1.1.0-rc.1` and are marked as prereleases.

- **Patch:** compatible bug fixes, security fixes, and performance improvements.
- **Minor:** compatible new tools, optional arguments, configuration options, and output fields.
- **Major:** removing or renaming tools, changing required arguments, incompatible configuration or result shapes, or changing documented paging and filtering semantics.

Treat tool names, required arguments, documented output fields, configuration keys, and continuation offsets as public contracts. Additive result fields should be safe for clients that ignore unknown fields. Document any necessary migration before releasing a major version.

`main` holds the next release. Create a maintenance branch only when an older major version needs fixes independently of `main`. Never move or reuse a published tag, and never replace published release assets. Fix a bad release with a new patch version.

## Distribution

Each release contains:

| Asset | Consumer |
|---|---|
| `kafka-mcp_VERSION_OS_ARCH[.exe]` | Native binary; Linux/macOS/Windows, AMD64/ARM64 |
| `kafka-mcp_VERSION_OS_ARCH.mcpb` | Claude Desktop; macOS and Windows on both architectures |
| `kafka-mcp-VERSION.tgz` | Small Node.js launcher for editor install links |
| `SHA256SUMS` | SHA-256 integrity hashes for every downloadable asset |

Desktop bundles contain their executable and need no language runtime. The installer asks users to choose their existing Kafka configuration file. Bundles are unsigned; the client may ask users to approve the local extension.

The launcher requires Node.js 20+, downloads the matching executable from the exact GitHub tag, verifies its checksum, and keeps a version-specific cache in `~/.cache/kafka-mcp`. Set `KAFKA_MCP_CACHE_DIR` to change that location. Cached binaries are verified before reuse and downloaded again if corrupt. The launcher preserves stdio and passes command arguments and environment variables to the native server.

The launcher package is served directly from GitHub Releases, so installs do not depend on a maintainer's npm publishing credentials. An npm registry listing can be added later without changing the native server. Publishing release files does not automatically submit the server to client marketplaces or the MCP registry.

Install links pin the release version. Upgrades are deliberate: install the new bundle or use the new version's editor link. Kafka configuration, certificates, saved scans, and exports live outside the installation directory and can be retained. To roll back, install a previous published version and use its documented configuration format.

## Release checklist

1. Update `package.json` and `internal/config/normalize.go` to the same SemVer. A Go test checks they agree.
2. Add a dated entry to `CHANGELOG.md`, including any migration instructions and known limitations.
3. Run `python3 scripts/install_links.py` to update the README's pinned install links.
4. Run `make check`, `npm test`, and `python3 scripts/install_links.py --check`.
5. For a packaging preview, run `make release VERSION=1.0.0`. This requires Go 1.26.1, Node.js 20+, npm, and Python 3.11+. It cross-compiles without CGO, validates and packs desktop manifests using the pinned MCPB CLI, packages the launcher, and writes checksums. `dist/` must be empty to prevent mixing versions.
6. Run `python3 scripts/smoke.py --native-dist dist --version 1.0.0` on the native build. It checks the executable version, MCP initialization, tool discovery, and cluster discovery without needing a Kafka broker.
7. Commit, push `main`, and wait for CI to pass. Create and push the annotated tag:

   ```sh
   git tag -a v1.0.0 -m "Release v1.0.0"
   git push origin v1.0.0
   ```

8. Watch the **Release** workflow. It requires the tagged commit to belong to `main`, reruns checks, and builds all assets. Native smoke tests run on Linux, macOS, and Windows. Only after all checks pass does the publication job verify checksums, upload a draft, and publish it. Prereleases do not replace the latest stable release.
9. Download the published launcher package and run `npx --yes --package=<package URL> kafka-mcp -version`; verify the downloaded binary reports the release version. Test at least one MCP handshake through the launcher and check that the public release contains every expected asset.

If a workflow fails before publication, fix the cause and rerun it when appropriate. A rerun can replace assets only on an existing draft; it refuses to overwrite an already-published release. If the fix changes released source, create a new version rather than moving an existing tag.

## Checks and build settings

CI runs vet, lint, race-enabled Go tests, a native build, launcher tests, and install-link consistency checks. Release builds use Go 1.26.1 with `CGO_ENABLED=0`, `-mod=readonly`, `-trimpath`, disabled VCS stamping, an empty build ID, and the tag's version embedded via linker flags. GitHub Actions dependencies are pinned to commit SHAs. Only the publication job has permission to write releases.

Production HTTP deployment remains operator-managed. Use [the shared HTTP example](../examples/http-shared.yaml), configure authentication and network access for the intended Kafka clusters, and deploy a version-pinned binary. A GitHub release does not start a hosted service.
