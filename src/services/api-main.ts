import { loadConfig, log } from '../config';
import { buildApi } from './api';
import { installShutdown } from '../ops';

async function main(): Promise<void> {
  const cfg = loadConfig();
  const api = await buildApi(cfg);
  log('info', 'api listening', { port: cfg.API_PORT, shards: Object.keys(cfg.shardUrls) });
  installShutdown('api', async () => {
    await api.close();
  });
}

main().catch((err) => {
  log('error', 'api failed to start', { error: String(err), stack: (err as Error).stack });
  process.exit(1);
});
