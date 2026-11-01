'use strict';

const { createHash, randomUUID } = require('node:crypto');
const { createReadStream, createWriteStream } = require('node:fs');
const { mkdir, readFile, rename, unlink, writeFile } = require('node:fs/promises');
const { homedir } = require('node:os');
const { join } = require('node:path');
const { Readable, Transform } = require('node:stream');
const { pipeline } = require('node:stream/promises');

const RELEASES = 'https://github.com/Midwayne/kafka-mcp/releases/download';

function assetName(version, platform, arch) {
  if (!/^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?$/.test(version)) {
    throw new Error(`Invalid release version: ${version}`);
  }
  const os = { darwin: 'darwin', linux: 'linux', win32: 'windows' }[platform];
  const cpu = { x64: 'amd64', arm64: 'arm64' }[arch];
  if (!os || !cpu) throw new Error(`Unsupported platform: ${platform}/${arch}`);
  return `kafka-mcp_${version}_${os}_${cpu}${platform === 'win32' ? '.exe' : ''}`;
}

async function request(url) {
  const response = await fetch(url, { signal: AbortSignal.timeout(120_000) });
  if (!response.ok) throw new Error(`Download failed (${response.status}): ${url}`);
  return response;
}

async function checksum(url, asset) {
  const response = await request(url);
  const chunks = [];
  let size = 0;
  for await (const chunk of response.body) {
    size += chunk.length;
    if (size > 1024 * 1024) throw new Error('Checksum file exceeds 1 MiB');
    chunks.push(Buffer.from(chunk));
  }
  for (const line of Buffer.concat(chunks).toString('utf8').split('\n')) {
    const match = /^([a-f0-9]{64}) {2}(.+)$/.exec(line);
    if (match?.[2] === asset) return match[1];
  }
  throw new Error(`No checksum found for ${asset}`);
}

async function verified(file, expected) {
  if (!/^[a-f0-9]{64}$/.test(expected)) return false;
  try {
    const hash = createHash('sha256');
    for await (const chunk of createReadStream(file)) hash.update(chunk);
    return hash.digest('hex') === expected;
  } catch (error) {
    if (error.code === 'ENOENT') return false;
    throw error;
  }
}

async function download(url, destination, expected) {
  const response = await request(url);
  const hash = createHash('sha256');
  let size = 0;
  const check = new Transform({
    transform(chunk, encoding, callback) {
      size += chunk.length;
      if (size > 128 * 1024 * 1024) return callback(new Error('Binary exceeds 128 MiB'));
      hash.update(chunk);
      callback(null, chunk);
    },
  });
  await pipeline(Readable.fromWeb(response.body), check,
    createWriteStream(destination, { flags: 'wx', mode: 0o700 }));
  if (hash.digest('hex') !== expected) throw new Error('Downloaded binary failed SHA-256 verification');
}

async function ensureBinary({ version, platform = process.platform, arch = process.arch,
  cacheRoot = process.env.KAFKA_MCP_CACHE_DIR || join(homedir(), '.cache', 'kafka-mcp'),
  baseURL = `${RELEASES}/v${version}` }) {
  const asset = assetName(version, platform, arch);
  const directory = join(cacheRoot, version, `${platform}-${arch}`);
  const binary = join(directory, asset);
  const digestFile = `${binary}.sha256`;
  let cachedDigest = '';
  try { cachedDigest = (await readFile(digestFile, 'utf8')).trim(); }
  catch (error) { if (error.code !== 'ENOENT') throw error; }
  if (await verified(binary, cachedDigest)) return binary;

  await mkdir(directory, { recursive: true, mode: 0o700 });
  const expected = await checksum(`${baseURL}/SHA256SUMS`, asset);
  const temporary = `${binary}.${randomUUID()}.part`;
  const temporaryDigest = `${temporary}.sha256`;
  try {
    await download(`${baseURL}/${asset}`, temporary, expected);
    await rename(temporary, binary);
    await writeFile(temporaryDigest, expected, { flag: 'wx', mode: 0o600 });
    await rename(temporaryDigest, digestFile);
  } finally {
    await Promise.all([temporary, temporaryDigest].map((file) =>
      unlink(file).catch((error) => { if (error.code !== 'ENOENT') throw error; })));
  }
  return binary;
}

module.exports = { assetName, ensureBinary };
