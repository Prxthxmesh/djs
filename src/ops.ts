import Fastify, { FastifyInstance } from 'fastify';
import { Registry } from 'prom-client';

export async function startOpsServer(
  port: number,
  registry: Registry,
  health: () => Promise<{ ok: boolean; detail?: string }>,
  onMetrics?: () => Promise<void>,
): Promise<FastifyInstance> {
  const app = Fastify({ logger: false });
  app.get('/health', async (_req, reply) => {
    try {
      const h = await health();
      return reply.code(h.ok ? 200 : 503).send({ status: h.ok ? 'ok' : 'degraded', detail: h.detail });
    } catch (err) {
      return reply.code(503).send({ status: 'error', detail: String(err) });
    }
  });
  app.get('/metrics', async (_req, reply) => {
    if (onMetrics) await onMetrics();
    reply.header('content-type', registry.contentType);
    return registry.metrics();
  });
  await app.listen({ port, host: '0.0.0.0' });
  return app;
}

export const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms));

export function backoffWithJitter(attempt: number, minMs: number, maxMs: number, random = Math.random): number {
  const cap = Math.min(maxMs, minMs * 2 ** Math.max(0, attempt));
  return Math.floor(minMs + random() * Math.max(0, cap - minMs));
}

export function installShutdown(name: string, fn: (signal: string) => Promise<void>): void {
  let running = false;
  const handler = (signal: string) => {
    if (running) return;
    running = true;
    console.log(JSON.stringify({ ts: new Date().toISOString(), level: 'info', msg: 'shutdown', service: name, signal }));
    fn(signal)
      .then(() => process.exit(0))
      .catch((err) => {
        console.error(JSON.stringify({ ts: new Date().toISOString(), level: 'error', msg: 'shutdown failed', error: String(err) }));
        process.exit(1);
      });
  };
  process.on('SIGTERM', () => handler('SIGTERM'));
  process.on('SIGINT', () => handler('SIGINT'));
}
