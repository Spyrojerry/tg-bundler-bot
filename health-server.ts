import { createServer, Server } from 'http';
import { createLogger } from './logger';

const log = createLogger('HTTP');

export function startHealthServer(port: number): Server {
  const server = createServer((req, res) => {
    if (req.url === '/health' || req.url === '/') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ ok: true }));
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
