import { loadConfig, log } from '../config';
import { SchedulerService } from './scheduler';
import { installShutdown, startOpsServer } from '../ops';

async function main(): Promise<void> {
  const cfg = loadConfig();
  const scheduler = new SchedulerService(cfg);
  await scheduler.start();
  const ops = await startOpsServer(cfg.OPS_PORT, scheduler.metrics.registry, () => scheduler.health());
  log('info', 'scheduler started', { schedulerId: scheduler.id, opsPort: cfg.OPS_PORT });
  installShutdown('scheduler', async () => {
    await scheduler.stop();
    await ops.close();
  });
}

main().catch((err) => {
  log('error', 'scheduler failed to start', { error: String(err), stack: (err as Error).stack });
  process.exit(1);
});
