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
    await sleep(25);
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

type StateResponse = {
  markdown?: string;
  content?: string;
  readSource?: string;
  projectionFresh?: boolean;
  repairPending?: boolean;
  marks?: Record<string, { kind?: string; status?: string }>;
};

const CLIENT_HEADERS = {
  'X-Proof-Client-Version': '0.31.2',
  'X-Proof-Client-Build': 'tests',
  'X-Proof-Client-Protocol': '3',
};

async function runCollabPersistProjectionVersionSyncGuard(): Promise<void> {
  const dbName = `proof-collab-persist-projection-version-sync-${Date.now()}-${Math.random().toString(36).slice(2)}.db`;
  const dbPath = path.join(os.tmpdir(), dbName);
  process.env.DATABASE_PATH = dbPath;
  process.env.COLLAB_EMBEDDED_WS = '1';

  const [{ apiRoutes }, { agentRoutes }, { setupWebSocket }, collab, db] = await Promise.all([
    import('../../server/routes.js'),
    import('../../server/agent-routes.js'),
    import('../../server/ws.js'),
    import('../../server/collab.js'),
    import('../../server/db.js'),
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
        title: 'Collab persist projection version sync guard',
        markdown: '# Guard\n\nAlpha Beta\n',
        marks: {},
      }),
    });
    const created = await mustJson<CreateResponse>(createRes, 'create');

    const suggestReplaceRes = await fetch(`${httpBase}/api/agent/${created.slug}/marks/suggest-replace`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
        'x-share-token': created.ownerSecret,
      },
      body: JSON.stringify({
        quote: 'Alpha',
        content: 'Gamma',
        by: 'ai:reviewer',
      }),
    });
    const suggestReplace = await mustJson<SuggestResponse>(suggestReplaceRes, 'suggest-replace');
    const acceptedMarkId = Object.entries(suggestReplace.marks ?? {}).find(([, m]) => m?.kind === 'replace')?.[0] ?? '';
    assert(acceptedMarkId.length > 0, 'Expected replace mark id');

    const acceptRes = await fetch(`${httpBase}/api/agent/${created.slug}/marks/accept`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
        'x-share-token': created.ownerSecret,
      },
      body: JSON.stringify({ markId: acceptedMarkId, by: 'human:editor' }),
    });
    await mustJson<{ success: boolean }>(acceptRes, 'accept');

    const beforeProjected = db.getProjectedDocumentBySlug(created.slug);
    assert(Boolean(beforeProjected), 'Expected projected document before collab attr update');
    const beforeYStateVersion = beforeProjected!.y_state_version;
    assert(
      beforeProjected!.projection_y_state_version === beforeYStateVersion,
      `Expected synced y_state_version before collab update, got doc=${beforeYStateVersion} proj=${String(beforeProjected!.projection_y_state_version)}`,
    );

    const sessionRes = await fetch(`${httpBase}/api/documents/${created.slug}/collab-session`, {
      headers: { ...CLIENT_HEADERS, 'x-share-token': created.ownerSecret },
    });
    const session = await mustJson<CollabSessionResponse>(sessionRes, 'collab-session');
    assert(session.success === true, 'Expected collab-session success');

    ydoc = new Y.Doc();
    provider = new HocuspocusProvider({
      url: normalizeWsBase(session.session.collabWsUrl),
      name: session.session.slug,
      document: ydoc,
      parameters: {
        token: session.session.token,
        role: session.session.role,
      },
      token: session.session.token,
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
    await waitFor(() => ydoc!.getXmlFragment('prosemirror').length > 0, 5_000, 'prosemirror populated');

    // Trigger a Yjs update whose derived markdown and marks remain unchanged,
    // exercising the persistDoc unchanged-and-fresh skip path.
    const fragment = ydoc.getXmlFragment('prosemirror');
    let touched = false;
    for (const child of fragment.toArray()) {
      if (child instanceof Y.XmlElement) {
        ydoc.transact(() => {
          child.setAttribute('data-guard-sync', '1');
        }, 'guard-unchanged-attr-update');
        touched = true;
        break;
      }
    }
    assert(touched, 'Expected an XmlElement in prosemirror fragment');

    await waitFor(() => provider!.unsyncedChanges === 0, 5_000, 'provider unsyncedChanges flush');
    await waitFor(
      () => (db.getProjectedDocumentBySlug(created.slug)?.y_state_version ?? 0) > beforeYStateVersion,
      5_000,
      'persistDoc advancing y_state_version',
    );

    const afterProjected = db.getProjectedDocumentBySlug(created.slug);
    assert(Boolean(afterProjected), 'Expected projected document after collab attr update');
    assert(
      afterProjected!.projection_y_state_version === afterProjected!.y_state_version,
      `Expected document_projections.y_state_version (${String(afterProjected!.projection_y_state_version)}) to match documents.y_state_version (${afterProjected!.y_state_version})`,
    );

    const stateRes = await fetch(`${httpBase}/api/agent/${created.slug}/state`, {
      headers: { ...CLIENT_HEADERS, 'x-share-token': created.ownerSecret },
    });
    const state = await mustJson<StateResponse>(stateRes, 'state');
    assert(state.readSource === 'projection', `Expected readSource=projection, got ${String(state.readSource)}`);
    assert(state.projectionFresh === true, `Expected projectionFresh=true, got ${String(state.projectionFresh)}`);
    assert(state.repairPending !== true, `Expected repairPending not true, got ${String(state.repairPending)}`);

    const suggestDeleteRes = await fetch(`${httpBase}/api/agent/${created.slug}/marks/suggest-delete`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
        'x-share-token': created.ownerSecret,
      },
      body: JSON.stringify({
        quote: 'Beta',
        by: 'ai:reviewer',
      }),
    });
    const suggestDelete = await mustJson<SuggestResponse>(suggestDeleteRes, 'suggest-delete');
    const rejectedMarkId = Object.entries(suggestDelete.marks ?? {}).find(([, m]) => m?.kind === 'delete')?.[0] ?? '';
    assert(rejectedMarkId.length > 0, 'Expected delete mark id');

    const rejectRes = await fetch(`${httpBase}/api/agent/${created.slug}/marks/reject`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
        'x-share-token': created.ownerSecret,
      },
      body: JSON.stringify({ markId: rejectedMarkId, by: 'human:editor' }),
    });
    await mustJson<{ success: boolean }>(rejectRes, 'reject');

    // Guard: mergePreservedActionMarks must scrub tombstoned accepted/rejected marks
    const mergedWithSuggestions = collab.mergePreservedActionMarks(
      created.slug,
      {
        [acceptedMarkId]: { kind: 'replace', by: 'ai:stale', status: 'pending', quote: 'Alpha', content: 'Gamma' },
        [rejectedMarkId]: { kind: 'delete', by: 'ai:stale', status: 'pending', quote: 'Beta' },
      },
      { includeSuggestions: true },
    );
    assert(!mergedWithSuggestions[acceptedMarkId], 'Expected accepted tombstoned mark scrubbed from mergePreservedActionMarks');
    assert(!mergedWithSuggestions[rejectedMarkId], 'Expected rejected tombstoned mark scrubbed from mergePreservedActionMarks');

    const mergedDefault = collab.mergePreservedActionMarks(created.slug, {
      [acceptedMarkId]: { kind: 'replace', by: 'ai:stale', status: 'pending', quote: 'Alpha', content: 'Gamma' },
      [rejectedMarkId]: { kind: 'delete', by: 'ai:stale', status: 'pending', quote: 'Beta' },
    });
    assert(!mergedDefault[acceptedMarkId], 'Expected accepted tombstoned mark scrubbed with default options');
    assert(!mergedDefault[rejectedMarkId], 'Expected rejected tombstoned mark scrubbed with default options');

    console.log('✓ collab persist keeps projection y_state_version synced and scrubs tombstoned marks');
  } finally {
    if (provider) {
      try {
        provider.disconnect();
        provider.destroy();
      } catch {
        // ignore
      }
    }
    if (ydoc) {
      try {
        ydoc.destroy();
      } catch {
        // ignore
      }
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

runCollabPersistProjectionVersionSyncGuard()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err instanceof Error ? err.message : String(err));
    process.exit(1);
  });
