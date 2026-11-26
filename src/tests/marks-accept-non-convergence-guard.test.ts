import { unlinkSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { createServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import express from 'express';
import * as Y from 'yjs';
import { HocuspocusProvider } from '@hocuspocus/provider';
import { WebSocketServer } from 'ws';

function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(message);
}

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs: number,
  label: string,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() <= deadline) {
    if (await predicate()) return;
    await sleep(10);
  }
  throw new Error(`Timed out waiting for ${label}`);
}

async function mustJson<T>(res: Response, label: string): Promise<T> {
  const text = await res.text().catch(() => '');
  if (!res.ok) throw new Error(`${label}: HTTP ${res.status}: ${text.slice(0, 500)}`);
  return JSON.parse(text) as T;
}

function normalizeWsBase(collabWsUrl: string): string {
  const raw = collabWsUrl.replace(/\?slug=.*$/, '');
  try {
    const url = new URL(raw);
    if (url.hostname === 'localhost') url.hostname = '127.0.0.1';
    return url.toString();
  } catch {
    return raw.replace('ws://localhost:', 'ws://127.0.0.1:');
  }
}

type CreateResponse = { slug: string; ownerSecret: string };

type SuggestResponse = {
  marks?: Record<string, { kind?: string; content?: string }>;
};

type CollabSessionResponse = {
  success: boolean;
  session: {
    collabWsUrl: string;
    slug: string;
    token: string;
    role: string;
  };
};

type AcceptFailureResponse = {
  success?: boolean;
  code?: string;
  reason?: string;
  collab?: {
    status?: string;
    reason?: string;
  };
};

const CLIENT_HEADERS = {
  'X-Proof-Client-Version': '0.31.2',
  'X-Proof-Client-Build': 'tests',
  'X-Proof-Client-Protocol': '3',
};

async function run(): Promise<void> {
  const dbName = `proof-marks-accept-non-convergence-guard-${Date.now()}-${Math.random().toString(36).slice(2)}.db`;
  const dbPath = path.join(os.tmpdir(), dbName);
  process.env.DATABASE_PATH = dbPath;
  process.env.COLLAB_EMBEDDED_WS = '1';
  process.env.AGENT_EDIT_COLLAB_STABILITY_MS = '800';
  process.env.AGENT_EDIT_COLLAB_STABILITY_SAMPLE_MS = '50';

  const [{ apiRoutes }, { agentRoutes }, { setupWebSocket }, collab, db, canonical] = await Promise.all([
    import('../../server/routes.js'),
    import('../../server/agent-routes.js'),
    import('../../server/ws.js'),
    import('../../server/collab.js'),
    import('../../server/db.js'),
    import('../../server/canonical-document.js'),
  ]);

  const app = express();
  app.use(express.json({ limit: '2mb' }));
  app.use('/api', apiRoutes);
  app.use('/api/agent', agentRoutes);

  const server = createServer(app);
  const wss = new WebSocketServer({ server, path: '/ws' });
  setupWebSocket(wss);

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()));
  const address = server.address() as AddressInfo;
  const httpBase = `http://127.0.0.1:${address.port}`;
  await collab.startCollabRuntimeEmbedded(address.port);

  let provider: HocuspocusProvider | null = null;
  let ydoc: Y.Doc | null = null;

  try {
    const createRes = await fetch(`${httpBase}/api/documents`, {
      method: 'POST',
      headers: { ...CLIENT_HEADERS, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        title: 'Marks accept non-convergence guard',
        markdown: '# Draft\n\nGood\n',
        marks: {},
      }),
    });
    const created = await mustJson<CreateResponse>(createRes, 'create');

    const suggestRes = await fetch(`${httpBase}/api/agent/${created.slug}/marks/suggest-replace`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
        'x-share-token': created.ownerSecret,
      },
      body: JSON.stringify({
        quote: 'Good',
        content: 'Bad!',
        by: 'ai:reviewer',
      }),
    });
    const suggestPayload = await mustJson<SuggestResponse>(suggestRes, 'suggest-replace');
    const suggestionId = Object.entries(suggestPayload.marks ?? {}).find(([, m]) => m?.kind === 'replace')?.[0] ?? '';
    assert(suggestionId.length > 0, 'Expected replace suggestion id');

    const collabSessionRes = await fetch(`${httpBase}/api/documents/${created.slug}/collab-session`, {
      headers: { ...CLIENT_HEADERS, 'x-share-token': created.ownerSecret },
    });
    const collabSession = await mustJson<CollabSessionResponse>(collabSessionRes, 'collab-session');
    assert(collabSession.success === true, 'Expected successful collab session');

    ydoc = new Y.Doc();
    provider = new HocuspocusProvider({
      url: normalizeWsBase(collabSession.session.collabWsUrl),
      name: collabSession.session.slug,
      document: ydoc,
      parameters: {
        token: collabSession.session.token,
        role: collabSession.session.role,
      },
      token: collabSession.session.token,
      preserveConnection: false,
      broadcast: false,
    });

    let connected = false;
    let synced = false;
    provider.on('status', (event: { status: string }) => {
      if (event.status === 'connected') connected = true;
    });
    provider.on('synced', (event: { state?: boolean }) => {
      if (event.state !== false) synced = true;
    });

    await waitFor(() => connected && synced, 10_000, 'provider connected+synced');

    const divergePromise = (async () => {
      await waitFor(
        () => Boolean(db.getDocumentBySlug(created.slug)?.markdown?.includes('Bad!')),
        5_000,
        'accepted markdown persisted before stability divergence',
      );
      const diverged = await canonical.mutateCanonicalDocument({
        slug: created.slug,
        nextMarkdown: '# Draft\n\nDiverged\n',
        nextMarks: {},
        source: 'test:post-barrier-non-convergence',
        strictLiveDoc: false,
      });
      assert(diverged.ok, `Expected divergent canonical mutation to succeed, got ${diverged.ok ? 'ok' : diverged.code}`);
    })();

    const acceptRes = await fetch(`${httpBase}/api/agent/${created.slug}/marks/accept`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
        'x-share-token': created.ownerSecret,
      },
      body: JSON.stringify({
        markId: suggestionId,
        by: 'human:editor',
      }),
    });

    await divergePromise;

    const acceptBody = await acceptRes.json() as AcceptFailureResponse;
    assert(acceptRes.status === 409, `Expected HTTP 409 for genuine non-convergence, got ${acceptRes.status}`);
    assert(acceptBody.success === false, 'Expected success=false for non-convergence');
    assert(acceptBody.code === 'COLLAB_SYNC_FAILED', `Expected COLLAB_SYNC_FAILED, got ${String(acceptBody.code)}`);
    assert(
      acceptBody.collab?.status === 'pending',
      `Expected collab.status=pending, got ${String(acceptBody.collab?.status)}`,
    );

    console.log('✓ marks/accept returns 409 COLLAB_SYNC_FAILED on genuine post-barrier non-convergence');
  } finally {
    try {
      provider?.disconnect();
      provider?.destroy();
    } catch {
      // ignore
    }
    try {
      ydoc?.destroy();
    } catch {
      // ignore
    }
    await collab.stopCollabRuntime();
    try {
      wss.close();
    } catch {
      // ignore
    }
    await new Promise<void>((resolve) => server.close(() => resolve()));
    for (const suffix of ['', '-wal', '-shm']) {
      try {
        unlinkSync(`${dbPath}${suffix}`);
      } catch {
        // ignore
      }
    }
  }
}

run()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err instanceof Error ? err.message : String(err));
    process.exit(1);
  });
