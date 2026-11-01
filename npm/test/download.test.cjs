'use strict';

const assert = require('node:assert/strict');
const { createHash } = require('node:crypto');
const { mkdtemp, readFile, readdir, rm, stat, writeFile } = require('node:fs/promises');
const { createServer } = require('node:http');
const { tmpdir } = require('node:os');
const { dirname, join } = require('node:path');
const { test } = require('node:test');
const { assetName, ensureBinary } = require('../lib/download.cjs');

const version = '1.0.0';
const asset = assetName(version, 'linux', 'x64');
const payload = Buffer.from('a release binary');
const digest = createHash('sha256').update(payload).digest('hex');

async function fixture(t, { corrupt = false, missing = false } = {}) {
  const cacheRoot = await mkdtemp(join(tmpdir(), 'kafka-mcp-test-'));
  let requests = 0;
  const server = createServer((request, response) => {
    requests++;
    if (missing) { response.writeHead(404); response.end(); return; }
    response.end(request.url === '/SHA256SUMS' ? `${digest}  ${asset}\n` :
      (corrupt ? Buffer.from('corrupted') : payload));
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  t.after(async () => {
    await new Promise((resolve) => server.close(resolve));
    await rm(cacheRoot, { recursive: true, force: true });
  });
  return { options: { version, platform: 'linux', arch: 'x64', cacheRoot,
    baseURL: `http://127.0.0.1:${server.address().port}` }, requests: () => requests };
}

test('maps every supported OS and architecture and rejects unsupported targets', () => {
  for (const platform of ['linux', 'darwin', 'win32']) {
    for (const arch of ['x64', 'arm64']) {
      assert.match(assetName(version, platform, arch), /^kafka-mcp_1\.0\.0_/);
    }
  }
  assert.equal(assetName(version, 'win32', 'arm64'), 'kafka-mcp_1.0.0_windows_arm64.exe');
  assert.throws(() => assetName(version, 'linux', 'ia32'), /Unsupported platform/);
  assert.throws(() => assetName('../escape', 'linux', 'x64'), /Invalid release version/);
});

test('verifies downloads, caches them, and repairs corrupted cache entries', async (t) => {
  const f = await fixture(t);
  const binary = await ensureBinary(f.options);
  assert.deepEqual(await readFile(binary), payload);
  if (process.platform !== 'win32') assert.equal((await stat(binary)).mode & 0o777, 0o700);
  assert.equal(await ensureBinary(f.options), binary);
  assert.equal(f.requests(), 2);
  await writeFile(binary, 'corrupted cache');
  await ensureBinary(f.options);
  assert.deepEqual(await readFile(binary), payload);
  assert.equal(f.requests(), 4);
});

test('rejects mismatched checksums without leaving a usable binary', async (t) => {
  const f = await fixture(t, { corrupt: true });
  await assert.rejects(ensureBinary(f.options), /SHA-256 verification/);
  const directory = join(f.options.cacheRoot, version, 'linux-x64');
  assert.deepEqual(await readdir(directory), []);
});

test('reports missing releases without installing anything', async (t) => {
  const f = await fixture(t, { missing: true });
  await assert.rejects(ensureBinary(f.options), /Download failed \(404\)/);
});

test('concurrent installs share a complete verified binary', async (t) => {
  const f = await fixture(t);
  const paths = await Promise.all(Array.from({ length: 8 }, () => ensureBinary(f.options)));
  assert.ok(paths.every((path) => path === paths[0]));
  assert.deepEqual(await readFile(paths[0]), payload);
  assert.deepEqual((await readdir(dirname(paths[0]))).sort(), [asset, `${asset}.sha256`]);
});
