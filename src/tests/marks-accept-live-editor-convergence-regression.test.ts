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

type XmlSnapshotNode =
  | { kind: 'element'; nodeName: string; attrs: Record<string, unknown>; children: XmlSnapshotNode[] }
  | { kind: 'text'; delta: Array<{ insert: unknown; attributes?: Record<string, unknown> }> };

function snapshotXmlNode(node: unknown): XmlSnapshotNode {
  if (node instanceof Y.XmlText) {
    return {
      kind: 'text',
      delta: node.toDelta() as Array<{ insert: unknown; attributes?: Record<string, unknown> }>,
    };
  }
  if (node instanceof Y.XmlElement) {
    return {
      kind: 'element',
      nodeName: node.nodeName,
      attrs: node.getAttributes(),
      children: node.toArray().map((child) => snapshotXmlNode(child)),
    };
  }
  throw new Error(`Unsupported XML child node in snapshot: ${String(node)}`);
}

function buildXmlNode(snapshot: XmlSnapshotNode): Y.XmlElement | Y.XmlText {
  if (snapshot.kind === 'text') {
    const text = new Y.XmlText();
    text.applyDelta(snapshot.delta);
    return text;
  }
  const el = new Y.XmlElement(snapshot.nodeName);
  for (const [key, value] of Object.entries(snapshot.attrs)) {
    el.setAttribute(key, value as string);
  }
  if (snapshot.children.length > 0) {
    el.insert(0, snapshot.children.map((child) => buildXmlNode(child)));
  }
  return el;
}

function snapshotXmlFragment(fragment: Y.XmlFragment): XmlSnapshotNode[] {
  return fragment.toArray().map((child) => snapshotXmlNode(child));
}

function restoreXmlFragment(fragment: Y.XmlFragment, snapshot: XmlSnapshotNode[]): void {
  if (fragment.length > 0) {
    fragment.delete(0, fragment.length);
  }
  if (snapshot.length > 0) {
    fragment.insert(0, snapshot.map((node) => buildXmlNode(node)));
  }
}

type CreateResponse = { slug: string; ownerSecret: string };

type SuggestResponse = {
  marks?: Record<string, { kind?: string; content?: string; status?: string }>;
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
  marks?: Record<string, { kind?: string; status?: string }>;
  mutationBase?: { token?: string };
};

type AcceptResponse = {
  success?: boolean;
  code?: string;
  error?: string;
  reason?: string;
  markdown?: string;
  content?: string;
  marks?: Record<string, { kind?: string; status?: string }>;
  collab?: {
    status?: string;
    reason?: string;
    markdownConfirmed?: boolean | null;
    fragmentConfirmed?: boolean | null;
    canonicalConfirmed?: boolean | null;
  };
};

const CLIENT_HEADERS = {
  'X-Proof-Client-Version': '0.31.2',
  'X-Proof-Client-Build': 'tests',
  'X-Proof-Client-Protocol': '3',
};

async function connectCollabClient(
  httpBase: string,
  slug: string,
  ownerSecret: string,
): Promise<{
  ydoc: Y.Doc;
  provider: HocuspocusProvider;
  disconnect: () => void;
}> {
  const collabSessionRes = await fetch(`${httpBase}/api/documents/${slug}/collab-session`, {
    headers: { ...CLIENT_HEADERS, 'x-share-token': ownerSecret },
  });
  const collabSession = await mustJson<CollabSessionResponse>(collabSessionRes, 'collab-session');
  assert(collabSession.success === true, 'Expected successful collab session');

  const ydoc = new Y.Doc();
  const provider = new HocuspocusProvider({
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

  await waitFor(() => connected && synced, 10_000, `provider connected+synced for ${slug}`);

  return {
    ydoc,
    provider,
    disconnect: () => {
      try {
        provider.disconnect();
        provider.destroy();
      } catch {
        // ignore
      }
      try {
        ydoc.destroy();
      } catch {
        // ignore
      }
    },
  };
}

function attachStaleEditorWritebackObserver(ydoc: Y.Doc): void {
  const prosemirrorFragment = ydoc.getXmlFragment('prosemirror');
  const marksMap = ydoc.getMap('marks');
  const staleFragmentSnapshot = snapshotXmlFragment(prosemirrorFragment);
  let revertedOnce = false;

  // Registered before any binding update handler, matching collabClient.connect()
  // registering marksMap.observe before ProsemirrorBinding._typeChanged.
  marksMap.observe((_event, transaction) => {
    if (transaction.local || revertedOnce) return;
    revertedOnce = true;
    ydoc.transact(() => {
      restoreXmlFragment(prosemirrorFragment, staleFragmentSnapshot);
    }, 'y-prosemirror-stale-writeback');
  });
}

async function runLiveEditorAcceptConvergenceTest(): Promise<void> {
  const dbName = `proof-marks-accept-live-editor-convergence-${Date.now()}-${Math.random().toString(36).slice(2)}.db`;
  const dbPath = path.join(os.tmpdir(), dbName);
  process.env.DATABASE_PATH = dbPath;
  process.env.COLLAB_EMBEDDED_WS = '1';
  process.env.AGENT_EDIT_COLLAB_STABILITY_MS = '500';
  process.env.AGENT_EDIT_COLLAB_STABILITY_SAMPLE_MS = '50';

  const [{ apiRoutes }, { agentRoutes }, { setupWebSocket }, collab] = await Promise.all([
    import('../../server/routes.js'),
    import('../../server/agent-routes.js'),
    import('../../server/ws.js'),
    import('../../server/collab.js'),
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

  const cleanups: Array<() => void> = [];

  try {
    // Pass 1: replace suggestion accept with live editor stale-fragment writeback race
    const createRes1 = await fetch(`${httpBase}/api/documents`, {
      method: 'POST',
      headers: { ...CLIENT_HEADERS, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        title: 'Live editor accept convergence Pass 1',
        markdown: '# Draft\n\nGood\n',
        marks: {},
      }),
    });
    const doc1 = await mustJson<CreateResponse>(createRes1, 'pass1 create');

    const suggest1Res = await fetch(`${httpBase}/api/agent/${doc1.slug}/marks/suggest-replace`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
        'x-share-token': doc1.ownerSecret,
      },
      body: JSON.stringify({
        quote: 'Good',
        content: 'Bad!',
        by: 'ai:reviewer',
      }),
    });
    const suggest1 = await mustJson<SuggestResponse>(suggest1Res, 'pass1 suggest-replace');
    const replaceMarkId = Object.entries(suggest1.marks ?? {}).find(([, m]) => m?.kind === 'replace')?.[0] ?? '';
    assert(replaceMarkId.length > 0, 'Pass 1 expected replace suggestion id');

    const client1 = await connectCollabClient(httpBase, doc1.slug, doc1.ownerSecret);
    cleanups.push(client1.disconnect);

    await waitFor(
      () => client1.ydoc.getXmlFragment('prosemirror').length > 0 && client1.ydoc.getMap('marks').has(replaceMarkId),
      5_000,
      'pass1 initial prosemirror and marks sync',
    );
    attachStaleEditorWritebackObserver(client1.ydoc);

    const preState1Res = await fetch(`${httpBase}/api/agent/${doc1.slug}/state`, {
      headers: { ...CLIENT_HEADERS, 'x-share-token': doc1.ownerSecret },
    });
    const preState1 = await mustJson<StateResponse>(preState1Res, 'pass1 pre-state');
    const baseToken1 = preState1.mutationBase?.token;
    assert(typeof baseToken1 === 'string' && baseToken1.length > 0, 'Pass 1 expected mutationBase.token');

    const accept1Res = await fetch(`${httpBase}/api/agent/${doc1.slug}/marks/accept`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
        'x-share-token': doc1.ownerSecret,
      },
      body: JSON.stringify({
        markId: replaceMarkId,
        by: 'human:editor',
        baseToken: baseToken1,
      }),
    });
    const accept1Text = await accept1Res.text();
    const accept1Payload = JSON.parse(accept1Text) as AcceptResponse;
    assert(
      accept1Res.status === 200 && accept1Payload.success === true,
      `Pass 1 marks/accept failed: HTTP ${accept1Res.status} code=${String(accept1Payload.code)} reason=${String(accept1Payload.reason ?? accept1Payload.collab?.reason)} body=${accept1Text.slice(0, 300)}`,
    );
    assert(
      accept1Payload.collab?.status === 'confirmed' && accept1Payload.collab?.canonicalConfirmed === true,
      `Pass 1 expected confirmed collab status, got ${JSON.stringify(accept1Payload.collab)}`,
    );

    const state1Res = await fetch(`${httpBase}/api/agent/${doc1.slug}/state`, {
      headers: { ...CLIENT_HEADERS, 'x-share-token': doc1.ownerSecret },
    });
    const state1 = await mustJson<StateResponse>(state1Res, 'pass1 post-state');
    const markdown1 = typeof state1.markdown === 'string' ? state1.markdown : (state1.content ?? '');
    assert(markdown1.includes('Bad!') && !markdown1.includes('Good'), `Pass 1 expected Bad! without Good, got ${JSON.stringify(markdown1)}`);
    assert(!state1.marks?.[replaceMarkId], 'Pass 1 expected pending replace suggestion cleared');
    assert(
      Object.values(state1.marks ?? {}).some((m) => m?.kind === 'authored'),
      'Pass 1 expected authored mark present',
    );
    assert(state1.readSource === 'projection', `Pass 1 expected readSource=projection, got ${String(state1.readSource)}`);
    assert(state1.projectionFresh === true, `Pass 1 expected projectionFresh=true, got ${String(state1.projectionFresh)}`);

    client1.disconnect();

    // Pass 2: second-rewrite insert then delete variant with live collab client + reconnect
    const createRes2 = await fetch(`${httpBase}/api/documents`, {
      method: 'POST',
      headers: { ...CLIENT_HEADERS, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        title: 'Live editor accept convergence Pass 2',
        markdown: '# Draft\n\nAlpha Beta\n',
        marks: {},
      }),
    });
    const doc2 = await mustJson<CreateResponse>(createRes2, 'pass2 create');

    const suggestInsertRes = await fetch(`${httpBase}/api/agent/${doc2.slug}/marks/suggest-insert`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
        'x-share-token': doc2.ownerSecret,
      },
      body: JSON.stringify({
        quote: 'Alpha',
        content: ' Inserted',
        by: 'ai:reviewer',
      }),
    });
    const suggestInsert = await mustJson<SuggestResponse>(suggestInsertRes, 'pass2 suggest-insert');
    const insertMarkId = Object.entries(suggestInsert.marks ?? {}).find(([, m]) => m?.kind === 'insert')?.[0] ?? '';
    assert(insertMarkId.length > 0, 'Pass 2 expected insert suggestion id');

    const suggestDeleteRes = await fetch(`${httpBase}/api/agent/${doc2.slug}/marks/suggest-delete`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
        'x-share-token': doc2.ownerSecret,
      },
      body: JSON.stringify({
        quote: 'Beta',
        by: 'ai:reviewer',
      }),
    });
    const suggestDelete = await mustJson<SuggestResponse>(suggestDeleteRes, 'pass2 suggest-delete');
    const deleteMarkId = Object.entries(suggestDelete.marks ?? {}).find(([, m]) => m?.kind === 'delete')?.[0] ?? '';
    assert(deleteMarkId.length > 0, 'Pass 2 expected delete suggestion id');

    const client2a = await connectCollabClient(httpBase, doc2.slug, doc2.ownerSecret);
    cleanups.push(client2a.disconnect);

    await waitFor(
      () => client2a.ydoc.getXmlFragment('prosemirror').length > 0
        && client2a.ydoc.getMap('marks').has(insertMarkId)
        && client2a.ydoc.getMap('marks').has(deleteMarkId),
      5_000,
      'pass2 initial prosemirror and marks sync',
    );
    attachStaleEditorWritebackObserver(client2a.ydoc);

    const preState2aRes = await fetch(`${httpBase}/api/agent/${doc2.slug}/state`, {
      headers: { ...CLIENT_HEADERS, 'x-share-token': doc2.ownerSecret },
    });
    const preState2a = await mustJson<StateResponse>(preState2aRes, 'pass2 pre-state insert');

    const acceptInsertRes = await fetch(`${httpBase}/api/agent/${doc2.slug}/marks/accept`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
        'x-share-token': doc2.ownerSecret,
      },
      body: JSON.stringify({
        markId: insertMarkId,
        by: 'human:editor',
        baseToken: preState2a.mutationBase?.token,
      }),
    });
    const acceptInsertText = await acceptInsertRes.text();
    const acceptInsertPayload = JSON.parse(acceptInsertText) as AcceptResponse;
    assert(
      acceptInsertRes.status === 200 && acceptInsertPayload.success === true,
      `Pass 2 insert accept failed: HTTP ${acceptInsertRes.status} body=${acceptInsertText.slice(0, 300)}`,
    );

    const stateAfterInsertRes = await fetch(`${httpBase}/api/agent/${doc2.slug}/state`, {
      headers: { ...CLIENT_HEADERS, 'x-share-token': doc2.ownerSecret },
    });
    const stateAfterInsert = await mustJson<StateResponse>(stateAfterInsertRes, 'pass2 post-insert state');
    const mdAfterInsert = typeof stateAfterInsert.markdown === 'string'
      ? stateAfterInsert.markdown
      : (stateAfterInsert.content ?? '');
    assert(mdAfterInsert.includes('Alpha Inserted'), `Pass 2 expected Alpha Inserted, got ${JSON.stringify(mdAfterInsert)}`);
    assert(!stateAfterInsert.marks?.[insertMarkId], 'Pass 2 expected insert suggestion cleared');
    assert(
      Object.values(stateAfterInsert.marks ?? {}).some((m) => m?.kind === 'authored'),
      'Pass 2 expected authored mark after insert accept',
    );

    client2a.disconnect();

    // Reconnect a fresh collab client using a refreshed collab-session (matching browser reconnect on access_epoch bump)
    const client2b = await connectCollabClient(httpBase, doc2.slug, doc2.ownerSecret);
    cleanups.push(client2b.disconnect);

    await waitFor(
      () => client2b.ydoc.getText('markdown').toString().includes('Alpha Inserted')
        && client2b.ydoc.getMap('marks').has(deleteMarkId),
      5_000,
      'pass2 reconnected client sync',
    );
    attachStaleEditorWritebackObserver(client2b.ydoc);

    const acceptDeleteRes = await fetch(`${httpBase}/api/agent/${doc2.slug}/marks/accept`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
        'x-share-token': doc2.ownerSecret,
      },
      body: JSON.stringify({
        markId: deleteMarkId,
        by: 'human:editor',
        baseToken: stateAfterInsert.mutationBase?.token,
      }),
    });
    const acceptDeleteText = await acceptDeleteRes.text();
    const acceptDeletePayload = JSON.parse(acceptDeleteText) as AcceptResponse;
    assert(
      acceptDeleteRes.status === 200 && acceptDeletePayload.success === true,
      `Pass 2 delete accept failed: HTTP ${acceptDeleteRes.status} body=${acceptDeleteText.slice(0, 300)}`,
    );

    const stateAfterDeleteRes = await fetch(`${httpBase}/api/agent/${doc2.slug}/state`, {
      headers: { ...CLIENT_HEADERS, 'x-share-token': doc2.ownerSecret },
    });
    const stateAfterDelete = await mustJson<StateResponse>(stateAfterDeleteRes, 'pass2 post-delete state');
    const mdAfterDelete = typeof stateAfterDelete.markdown === 'string'
      ? stateAfterDelete.markdown
      : (stateAfterDelete.content ?? '');
    assert(
      mdAfterDelete.includes('Alpha Inserted') && !mdAfterDelete.includes('Beta'),
      `Pass 2 expected Alpha Inserted with Beta removed, got ${JSON.stringify(mdAfterDelete)}`,
    );
    assert(!stateAfterDelete.marks?.[deleteMarkId], 'Pass 2 expected delete suggestion cleared');
    assert(
      Object.values(stateAfterDelete.marks ?? {}).some((m) => m?.kind === 'authored'),
      'Pass 2 expected authored marks present',
    );
    assert(
      stateAfterDelete.readSource === 'projection',
      `Pass 2 expected readSource=projection, got ${String(stateAfterDelete.readSource)}`,
    );
    assert(
      stateAfterDelete.projectionFresh === true,
      `Pass 2 expected projectionFresh=true, got ${String(stateAfterDelete.projectionFresh)}`,
    );

    console.log('✓ live editor marks/accept convergence passed across replace and insert/delete passes');
  } finally {
    for (const cleanup of cleanups) {
      cleanup();
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

runLiveEditorAcceptConvergenceTest().catch((err) => {
  console.error(err instanceof Error ? err.message : String(err));
  process.exit(1);
});
