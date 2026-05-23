// ─────────────────────────────────────────────────────────────────────────────
//  logger.ts  —  Structured, levelled console logger
// ─────────────────────────────────────────────────────────────────────────────

const LEVELS = { debug: 0, info: 1, warn: 2, error: 3 } as const;
type Level = keyof typeof LEVELS;

let currentLevel: Level = 'info';

function pad2(n: number): string {
  return String(n).padStart(2, '0');
}

function timestamp(): string {
  const d = new Date();
  return (
    `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}` +
    `T${pad2(d.getUTCHours())}:${pad2(d.getUTCMinutes())}:${pad2(d.getUTCSeconds())}Z`
  );
}

function log(level: Level, label: string, message: string, data?: unknown): void {
  if (LEVELS[level] < LEVELS[currentLevel]) return;

  const ts = timestamp();
  const prefix = `[${ts}] [${level.toUpperCase().padEnd(5)}] [${label}]`;

  if (data !== undefined) {
    const extra =
      typeof data === 'object' ? JSON.stringify(data, null, 0) : String(data);
    process.stdout.write(`${prefix} ${message} ${extra}\n`);
  } else {
    process.stdout.write(`${prefix} ${message}\n`);
  }
}

export function setLogLevel(level: Level): void {
  currentLevel = level;
}

export function createLogger(label: string) {
  return {
    debug: (msg: string, data?: unknown) => log('debug', label, msg, data),
    info:  (msg: string, data?: unknown) => log('info',  label, msg, data),
    warn:  (msg: string, data?: unknown) => log('warn',  label, msg, data),
    error: (msg: string, data?: unknown) => log('error', label, msg, data),
  };
}

export type Logger = ReturnType<typeof createLogger>;
