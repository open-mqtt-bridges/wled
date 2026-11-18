import { connect, type MqttClient } from 'mqtt';
import assert from 'node:assert/strict';
import { execFileSync, spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import WebSocket, { WebSocketServer } from 'ws';

const host = '127.0.0.1';
const topic = 'wled/availability-fixture';
const bridgeId = 'availability-bridge';
const brokerImage = 'eclipse-mosquitto:2.0.22@sha256:81a6193ae693942a3231cd3ac52875b02b7e3a19da671b2fb4b55cc874c5b279';
const cleanup: (() => unknown)[] = [];

function docker(...args: string[]) {
  return execFileSync('docker', args, { encoding: 'utf8', timeout: 60_000 }).trim();
}

async function waitFor(description: string, ready: () => boolean, timeout = 5_000) {
  const deadline = performance.now() + timeout;
  while (!ready()) {
    assert(performance.now() < deadline, `Timed out waiting for ${description}`);
    await delay(50);
  }
}

async function observe(url: string, filter: string) {
  const client = connect(url, { connectTimeout: 5_000, reconnectPeriod: 0 });
  cleanup.push(() => closeClient(client));
  const messages: { payload: string; retained: boolean }[] = [];
  client.on('message', (_, payload, packet) => messages.push({ payload: payload.toString(), retained: packet.retain }));
  await once(client, 'connect', { signal: AbortSignal.timeout(5_000) });
  const [grant] = await client.subscribeAsync(filter);
  assert.equal(grant.qos, 0);
  return { client, messages };
}

async function closeClient(client: MqttClient) {
  await client.endAsync(true);
}

async function run() {
  const dir = await mkdtemp(join(tmpdir(), 'wled-availability-'));
  cleanup.push(() => rm(dir, { recursive: true, force: true }));

  const broker = docker(
    'run',
    '--detach',
    '--publish',
    `${host}::1883`,
    brokerImage,
    'sh',
    '-c',
    `printf '%s\\n' 'listener 1883' 'allow_anonymous true' 'persistence false' 'log_dest stdout' 'log_type all' > /tmp/fixture.conf && exec mosquitto -c /tmp/fixture.conf`,
  );
  cleanup.push(() => docker('rm', '--force', broker));
  const endpoint = docker('port', broker, '1883/tcp');
  const brokerLogs = () => docker('logs', broker);
  await waitFor('Mosquitto startup', () => brokerLogs().includes('running'));
  const early = await observe(`mqtt://${endpoint}`, `${topic}/connected`);

  const sockets: WebSocket[] = [];
  let snapshots = 0;
  const server = createServer((request, response) => {
    if (request.url !== '/json') return response.writeHead(404).end();
    snapshots++;
    response.writeHead(200, { 'Content-Type': 'application/json' }).end('{}');
  });
  const wss = new WebSocketServer({ noServer: true });
  let acceptDevice!: () => void;
  const bridgeReady = new Promise<void>((done) => (acceptDevice = done));
  server.on('upgrade', (request, socket, head) => {
    if (request.url !== '/ws') return socket.destroy();
    void bridgeReady.then(() => {
      if (!socket.destroyed) wss.handleUpgrade(request, socket, head, (ws) => sockets.push(ws));
    });
  });
  server.listen(0, host);
  cleanup.push(async () => {
    for (const socket of sockets) socket.terminate();
    wss.close();
    await new Promise<void>((done) => server.close(() => done()));
  });
  await once(server, 'listening');

  const config = join(dir, 'config.yml');
  await writeFile(
    config,
    JSON.stringify({
      mqtt: { host, port: Number(new URL(`mqtt://${endpoint}`).port), clientId: bridgeId, reconnectDelayMs: 250 },
      instances: [{ id: 'fixture', topic, host: `${host}:${(server.address() as AddressInfo).port}` }],
    }),
  );

  const bridge = spawn(process.execPath, [resolve('dist/index.js')], {
    cwd: dir,
    env: { ...process.env, NODE_ENV: 'production', CONFIG_FILE: config, HOST: host, PORT: '0', NO_PROXY: host },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  bridge.stdout.on('data', (chunk) => (output += chunk));
  bridge.stderr.on('data', (chunk) => (output += chunk));
  const exited = once(bridge, 'exit');
  void exited.catch(() => undefined);
  cleanup.push(async () => {
    bridge.kill('SIGKILL');
    await exited;
  });

  try {
    await waitFor('bridge MQTT subscription', () => brokerLogs().includes(`Sending SUBACK to ${bridgeId}`));
    acceptDevice();
    await waitFor('early subscriber live true', () => early.messages.some(({ payload }) => payload === 'true'));
    assert.deepEqual(
      early.messages.find(({ payload }) => payload === 'true'),
      { payload: 'true', retained: false },
    );
    await waitFor('WLED snapshot', () => snapshots === 1);
    assert.equal(sockets.length, 1);
    assert.equal(sockets[0].readyState, WebSocket.OPEN);
    console.log('Early subscriber received live non-retained true on the original WLED socket.');

    const lastValues = new Map<string, boolean>([['stale', false]]);
    const late = await Promise.all(
      ['fresh', 'stale'].map(async (name) => ({
        name,
        ...(await observe(`mqtt://${endpoint}`, `${topic}/connected`)),
      })),
    );
    await Promise.all(
      late.map(async ({ name, messages }) => {
        await waitFor(`late subscriber (${name}) availability`, () => messages.length > 0, 35_000);
        assert.deepEqual(messages[0], { payload: 'true', retained: false });
        lastValues.set(name, messages[0].payload === 'true');
      }),
    );
    assert.equal(lastValues.get('fresh'), true);
    assert.equal(lastValues.get('stale'), true);
    assert.equal(sockets.length, 1);
    assert.equal(sockets[0].readyState, WebSocket.OPEN);
    assert.equal(snapshots, 1);
    console.log('Late subscribers recovered empty and stale false caches.');
  } catch (error) {
    console.error(output);
    throw error;
  }
}

void run()
  .catch((error: unknown) => {
    console.error(error);
    process.exitCode = 1;
  })
  .finally(async () => {
    for (const dispose of cleanup.reverse()) {
      try {
        await dispose();
      } catch (error) {
        console.error(error);
        process.exitCode = 1;
      }
    }
  });
