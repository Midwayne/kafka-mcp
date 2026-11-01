#!/usr/bin/env node
'use strict';

const { spawn } = require('node:child_process');
const { constants } = require('node:os');
const { ensureBinary } = require('../lib/download.cjs');
const { version } = require('../../package.json');

async function main() {
  const binary = await ensureBinary({ version });
  const child = spawn(binary, process.argv.slice(2), { stdio: 'inherit' });
  const signals = ['SIGINT', 'SIGTERM'];
  const forward = (signal) => { child.kill(signal); };
  for (const signal of signals) process.on(signal, forward);
  child.once('error', (error) => {
    console.error(`kafka-mcp: ${error.message}`);
    process.exitCode = 1;
  });
  child.once('exit', (code, signal) => {
    for (const name of signals) process.removeListener(name, forward);
    process.exitCode = code ?? (128 + (constants.signals[signal] ?? 1));
  });
}

main().catch((error) => {
  console.error(`kafka-mcp: ${error.message}`);
  process.exitCode = 1;
});
