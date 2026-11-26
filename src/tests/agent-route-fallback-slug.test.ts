import { readFileSync } from 'node:fs';
import path from 'node:path';

function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(message);
}

function run(): void {
  const source = readFileSync(path.resolve(process.cwd(), 'server/agent-routes.ts'), 'utf8');
  const start = source.indexOf('agentRoutes.use(async (req: Request, res: Response) => {');
  assert(start !== -1, 'Expected the agent route fallback handler');
  const block = source.slice(start, source.indexOf('  if (!checkAuth(req, res, slug', start));
  assert(
    block.includes("path.split('/')"),
    'Expected the agent route fallback to derive the slug from the path because router params are empty in middleware',
  );
  console.log('agent-route-fallback-slug: assertions passed');
}

run();
