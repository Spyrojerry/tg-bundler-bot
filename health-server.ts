import { createServer, Server } from 'http';
import { createLogger } from './logger';

const log = createLogger('HTTP');

export interface HealthStatus {
  ok: boolean;
  [key: string]: unknown;
}

export function startHealthServer(
  port: number,
  getStatus: () => HealthStatus = () => ({ ok: true }),
): Server {
  const server = createServer((req, res) => {
    if (req.url === '/health' || req.url === '/') {
      const status = getStatus();
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
