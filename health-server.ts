import { createServer, Server } from 'http';
import { createLogger } from './logger';

const log = createLogger('HTTP');

export interface HealthStatus {
  ok: boolean;
  [key: string]: unknown;
}

const SAMPLE_INTERVAL_MS = 30_000;
const LAG_CHECK_INTERVAL_MS = 1_000;
const LAG_WARN_THRESHOLD_MS = 200;

export interface ResourceStats {
  memory: {
    rssMb: number;
    heapUsedMb: number;
    heapTotalMb: number;
    externalMb: number;
  };
  eventLoopLagMs: number;
  sampledAt: number;
}

let lastStats: ResourceStats | null = null;
let lagTimer: NodeJS.Timeout | null = null;
let sampleTimer: NodeJS.Timeout | null = null;

function bytesToMb(bytes: number): number {
  return Math.round((bytes / (1024 * 1024)) * 10) / 10;
}

function sampleMemory(): ResourceStats['memory'] {
  const mem = process.memoryUsage();
  return {
    rssMb: bytesToMb(mem.rss),
    heapUsedMb: bytesToMb(mem.heapUsed),
    heapTotalMb: bytesToMb(mem.heapTotal),
    externalMb: bytesToMb(mem.external),
  };
}

export function startResourceMonitoring(): void {
  if (sampleTimer || lagTimer) return;

  let lastLagCheckAt = Date.now();
  let latestLagMs = 0;
  lagTimer = setInterval(() => {
    const now = Date.now();
    const expectedGap = now - lastLagCheckAt;
    latestLagMs = Math.max(0, expectedGap - LAG_CHECK_INTERVAL_MS);
    lastLagCheckAt = now;
    if (latestLagMs > LAG_WARN_THRESHOLD_MS) {
      log.warn('Event loop lag detected', { lagMs: latestLagMs });
    }
  }, LAG_CHECK_INTERVAL_MS);
  lagTimer.unref?.();

  sampleTimer = setInterval(() => {
    lastStats = {
      memory: sampleMemory(),
      eventLoopLagMs: latestLagMs,
      sampledAt: Date.now(),
    };
    log.info('Resource sample', lastStats);
  }, SAMPLE_INTERVAL_MS);
  sampleTimer.unref?.();
  lastStats = { memory: sampleMemory(), eventLoopLagMs: 0, sampledAt: Date.now() };
}

export function stopResourceMonitoring(): void {
  if (lagTimer) clearInterval(lagTimer);
  if (sampleTimer) clearInterval(sampleTimer);
  lagTimer = null;
  sampleTimer = null;
}

export function getLatestResourceStats(): ResourceStats | null {
  return lastStats;
}

export function startHealthServer(
  port: number,
  getStatus: () => HealthStatus = () => ({ ok: true }),
): Server {
  startResourceMonitoring();
  const server = createServer((req, res) => {
    if (req.url === '/health' || req.url === '/') {
      const status = { ...getStatus(), resources: getLatestResourceStats() };
      res.writeHead(status.ok ? 200 : 503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(status));
      return;
    }

    res.writeHead(404, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: false }));
  });

  server.listen(port, () => {
    log.info(`Health server listening on :${port}`);
  });

  return server;
}
