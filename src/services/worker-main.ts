import { loadConfig, log } from '../config';
import { WorkerService } from './worker';
import { installShutdown, startOpsServer } from '../ops';

async function main(): Promise<void> {
  const cfg = loadConfig();
  const worker = new WorkerService(cfg);
  await worker.start();
  const ops = await startOpsServer(cfg.OPS_PORT, worker.metrics.registry, () => worker.health());
  log('info', 'worker started', {
    workerId: worker.workerId,
    concurrency: cfg.WORKER_CONCURRENCY,
    shards: Object.keys(cfg.shardUrls),
    opsPort: cfg.OPS_PORT,
  });
  installShutdown('worker', async () => {
    await worker.stop();
    await ops.close();
  });
}

main().catch((err) => {
  log('error', 'worker failed to start', { error: String(err), stack: (err as Error).stack });
  process.exit(1);
});
