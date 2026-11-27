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

function hydrateSuggestionMarkInFragment(
  ydoc: Y.Doc,
  quote: string,
  suggestionAttrs: {
    id: string;
    kind: 'replace' | 'insert' | 'delete';
    by: string;
    content?: string;
  },
): boolean {
  const fragment = ydoc.getXmlFragment('prosemirror');
  const walk = (node: unknown): boolean => {
    if (node instanceof Y.XmlText) {
      const text = node.toString();
      const idx = text.indexOf(quote);
      if (idx >= 0) {
        ydoc.transact(() => {
          node.format(idx, quote.length, {
            proofSuggestion: {
              id: suggestionAttrs.id,
              kind: suggestionAttrs.kind,
              by: suggestionAttrs.by,
              ...(suggestionAttrs.content !== undefined ? { content: suggestionAttrs.content } : {}),
              status: 'pending',
            },
          });
        }, 'milkdown-suggestion-hydration');
        return true;
      }
      return false;
    }
    if (node instanceof Y.XmlElement || node instanceof Y.XmlFragment) {
      for (const child of node.toArray()) {
        if (walk(child)) return true;
      }
    }
    return false;
  };
  return walk(fragment);
}

function touchFragmentAttributes(ydoc: Y.Doc, attrValue: string): boolean {
  const fragment = ydoc.getXmlFragment('prosemirror');
  for (const child of fragment.toArray()) {
    if (child instanceof Y.XmlElement) {
      ydoc.transact(() => {
        child.setAttribute('data-proof-sync-epoch', attrValue);
      }, 'milkdown-post-sync-attr');
      return true;
    }
  }
  return false;
}

type CreateResponse = { slug: string; ownerSecret: string };

type BridgeSuggestResponse = {
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
  repairPending?: boolean;
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

type AutoReconnectClient = {
  getYdoc: () => Y.Doc;
  getProvider: () => HocuspocusProvider;
  isConnectedAndSynced: () => boolean;
  getReconnectCount: () => number;
  setReconnectDelayMs: (delayMs: number) => void;
  reemitStaleMarksAndFragment: () => void;
  disconnect: () => void;
};

async function connectCollabClient(
  httpBase: string,
  slug: string,
  ownerSecret: string,
  options: {
    autoReconnectOnClose?: boolean;
    reemitStaleMarksOnReconnect?: boolean;
    reemitFragmentSuggestionsOnReconnect?: boolean;
    touchFragmentAttrOnReconnect?: boolean;
    reconnectDelayMs?: number;
  } = {},
): Promise<AutoReconnectClient> {
  let disposed = false;
  let reconnectInFlight = false;
  let reconnectCount = 0;
  let reconnectDelayMs = options.reconnectDelayMs ?? 250;
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  let currentYdoc: Y.Doc | null = null;
  let currentProvider: HocuspocusProvider | null = null;
  let connected = false;
  let synced = false;
  const lastReceivedServerMarks: Record<string, unknown> = {};

  const reemitInto = (ydoc: Y.Doc) => {
    const marksMap = ydoc.getMap('marks');
    if (options.reemitStaleMarksOnReconnect && Object.keys(lastReceivedServerMarks).length > 0) {
      ydoc.transact(() => {
        for (const [markId, value] of Object.entries(lastReceivedServerMarks)) {
          if (!marksMap.has(markId)) {
            marksMap.set(markId, value);
          }
        }
      }, 'browser-last-received-marks-reemit');
    }
    if (options.reemitFragmentSuggestionsOnReconnect) {
      for (const [markId, raw] of Object.entries(lastReceivedServerMarks)) {
        if (!raw || typeof raw !== 'object' || Array.isArray(raw)) continue;
        const mark = raw as { kind?: unknown; quote?: unknown; by?: unknown; content?: unknown; status?: unknown };
        if (
          (mark.kind === 'replace' || mark.kind === 'insert' || mark.kind === 'delete')
          && typeof mark.quote === 'string'
          && mark.quote.length > 0
        ) {
          hydrateSuggestionMarkInFragment(ydoc, mark.quote, {
            id: markId,
            kind: mark.kind,
            by: typeof mark.by === 'string' && mark.by ? mark.by : 'ai:reviewer',
            ...(typeof mark.content === 'string' ? { content: mark.content } : {}),
          });
        }
      }
    }
  };

  const destroyCurrent = () => {
    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }
    if (currentProvider) {
      try {
        currentProvider.disconnect();
        currentProvider.destroy();
      } catch {
        // ignore
      }
    }
    if (currentYdoc) {
      try {
        currentYdoc.destroy();
      } catch {
        // ignore
      }
    }
    currentProvider = null;
    currentYdoc = null;
    connected = false;
    synced = false;
  };

  const connectOnce = async (isReconnect: boolean): Promise<void> => {
    if (disposed) return;
    destroyCurrent();

    const collabSessionRes = await fetch(`${httpBase}/api/documents/${slug}/collab-session`, {
      headers: { ...CLIENT_HEADERS, 'x-share-token': ownerSecret },
    });
    const collabSession = await mustJson<CollabSessionResponse>(collabSessionRes, 'collab-session');
    assert(collabSession.success === true, 'Expected successful collab session');

    const ydoc = new Y.Doc();
    currentYdoc = ydoc;

    const marksMap = ydoc.getMap('marks');
    const captureServerMarks = () => {
      marksMap.forEach((value, key) => {
        lastReceivedServerMarks[key] = value;
      });
    };
    marksMap.observe((_event, transaction) => {
      if (transaction.local) return;
      captureServerMarks();
    });

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
    currentProvider = provider;

    const triggerReconnect = () => {
      if (disposed || !options.autoReconnectOnClose || reconnectInFlight) return;
      reconnectInFlight = true;
      reconnectCount += 1;
      reconnectTimer = setTimeout(() => {
        reconnectTimer = null;
        void connectOnce(true).catch(() => {
          // retry loop handled by next disconnect or waitFor timeout
        }).finally(() => {
          reconnectInFlight = false;
        });
      }, reconnectDelayMs);
    };

    provider.on('status', (event: { status: string }) => {
      if (event.status === 'connected') {
        connected = true;
      } else if (event.status === 'disconnected') {
        connected = false;
        synced = false;
        triggerReconnect();
      }
    });
    provider.on('synced', (event: { state?: boolean }) => {
      if (event.state !== false) {
        synced = true;
        captureServerMarks();
        if (isReconnect) {
          reemitInto(ydoc);
          setTimeout(() => {
            if (!disposed && currentYdoc === ydoc && synced) {
              reemitInto(ydoc);
            }
          }, 150);
          if (options.touchFragmentAttrOnReconnect) {
            touchFragmentAttributes(ydoc, `reconnect-${reconnectCount}`);
          }
        }
      }
    });
    provider.on('close', () => {
      connected = false;
      synced = false;
      triggerReconnect();
    });
    provider.on('authenticationFailed', () => {
      connected = false;
      synced = false;
      triggerReconnect();
    });
  };

  await connectOnce(false);
  await waitFor(() => connected && synced, 10_000, `provider connected+synced for ${slug}`);

  return {
    getYdoc: () => {
      if (!currentYdoc) throw new Error('Collab client ydoc disposed');
      return currentYdoc;
    },
    getProvider: () => {
      if (!currentProvider) throw new Error('Collab client provider disposed');
      return currentProvider;
    },
    isConnectedAndSynced: () => connected && synced,
    getReconnectCount: () => reconnectCount,
    setReconnectDelayMs: (delayMs: number) => {
      reconnectDelayMs = delayMs;
    },
    reemitStaleMarksAndFragment: () => {
      if (currentYdoc) reemitInto(currentYdoc);
    },
    disconnect: () => {
      disposed = true;
      destroyCurrent();
    },
  };
}

async function runLiveMarksAcceptProjectionStaleRegressionTest(): Promise<void> {
  const dbName = `proof-marks-accept-live-projection-stale-${Date.now()}-${Math.random().toString(36).slice(2)}.db`;
  const dbPath = path.join(os.tmpdir(), dbName);
  process.env.DATABASE_PATH = dbPath;
  process.env.COLLAB_EMBEDDED_WS = '1';
  process.env.AGENT_EDIT_COLLAB_STABILITY_SAMPLE_MS = '50';

  const [{ apiRoutes }, { agentRoutes }, { createBridgeMountRouter }, { setupWebSocket }, collab] = await Promise.all([
    import('../../server/routes.js'),
    import('../../server/agent-routes.js'),
    import('../../server/bridge.js'),
    import('../../server/ws.js'),
    import('../../server/collab.js'),
  ]);

  const app = express();
  app.use(express.json({ limit: '2mb' }));
  app.use('/api', apiRoutes);
  app.use(apiRoutes);
  app.use('/api/agent', agentRoutes);
  app.use('/documents', createBridgeMountRouter());

  const server = createServer(app);
  const wss = new WebSocketServer({ server, path: '/ws' });
  setupWebSocket(wss);

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', () => resolve()));
  const address = server.address() as AddressInfo;
  const httpBase = `http://127.0.0.1:${address.port}`;
  await collab.startCollabRuntimeEmbedded(address.port);

  const cleanups: Array<() => void> = [];

  try {
    // Pass 1: bridge suggestion before live client connects + fragment <proofSuggestion> hydration
    const createRes1 = await fetch(`${httpBase}/documents`, {
      method: 'POST',
      headers: { ...CLIENT_HEADERS, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        title: 'Live marks projection Pass 1',
        markdown: '# Draft\n\nGood\n',
        marks: {},
      }),
    });
    const doc1 = await mustJson<CreateResponse>(createRes1, 'pass1 create');

    const suggest1Res = await fetch(`${httpBase}/documents/${doc1.slug}/bridge/suggestions`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        kind: 'replace',
        quote: 'Good',
        content: 'Bad!',
        by: 'ai:reviewer',
      }),
    });
    const suggest1 = await mustJson<BridgeSuggestResponse>(suggest1Res, 'pass1 bridge suggest replace');
    const replaceMarkId1 = Object.entries(suggest1.marks ?? {}).find(([, m]) => m?.kind === 'replace')?.[0] ?? '';
    assert(replaceMarkId1.length > 0, 'Pass 1 expected replace suggestion id from bridge');

    const client1 = await connectCollabClient(httpBase, doc1.slug, doc1.ownerSecret);
    cleanups.push(client1.disconnect);

    await waitFor(
      () => client1.getYdoc().getXmlFragment('prosemirror').length > 0,
      5_000,
      'pass1 initial prosemirror fragment sync',
    );

    const hydrated1 = hydrateSuggestionMarkInFragment(client1.getYdoc(), 'Good', {
      id: replaceMarkId1,
      kind: 'replace',
      by: 'ai:reviewer',
      content: 'Bad!',
    });
    assert(hydrated1, 'Pass 1 expected to hydrate <proofSuggestion> into prosemirror fragment');

    await waitFor(() => client1.getProvider().unsyncedChanges === 0, 5_000, 'pass1 client1 unsynced flush');
    await sleep(600);

    const accept1Res = await fetch(`${httpBase}/api/agent/${doc1.slug}/marks/accept`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
        'x-share-token': doc1.ownerSecret,
      },
      body: JSON.stringify({
        markId: replaceMarkId1,
        by: 'human:editor',
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
    assert(!state1.marks?.[replaceMarkId1], 'Pass 1 expected pending replace suggestion cleared');
    assert(
      Object.values(state1.marks ?? {}).some((m) => m?.kind === 'authored'),
      'Pass 1 expected authored mark present',
    );
    assert(state1.readSource === 'projection', `Pass 1 expected readSource=projection, got ${String(state1.readSource)}`);
    assert(state1.projectionFresh === true, `Pass 1 expected projectionFresh=true, got ${String(state1.projectionFresh)}`);

    client1.disconnect();

    // Pass 2: already-open auto-reconnecting live client + repeat bridge suggestions & accepts
    const createRes2 = await fetch(`${httpBase}/documents`, {
      method: 'POST',
      headers: { ...CLIENT_HEADERS, 'Content-Type': 'application/json' },
      body: JSON.stringify({
        title: 'Live marks projection Pass 2',
        markdown: '# Draft\n\nAlpha Beta\n',
        marks: {},
      }),
    });
    const doc2 = await mustJson<CreateResponse>(createRes2, 'pass2 create');

    const client2 = await connectCollabClient(httpBase, doc2.slug, doc2.ownerSecret, {
      autoReconnectOnClose: true,
      reemitStaleMarksOnReconnect: true,
      reemitFragmentSuggestionsOnReconnect: true,
      touchFragmentAttrOnReconnect: true,
    });
    cleanups.push(client2.disconnect);

    await waitFor(
      () => client2.getYdoc().getXmlFragment('prosemirror').length > 0,
      5_000,
      'pass2 initial prosemirror fragment sync',
    );

    const suggest2aRes = await fetch(`${httpBase}/documents/${doc2.slug}/bridge/suggestions`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        kind: 'replace',
        quote: 'Alpha',
        content: 'Gamma',
        by: 'ai:reviewer',
      }),
    });
    const suggest2a = await mustJson<BridgeSuggestResponse>(suggest2aRes, 'pass2 bridge suggest replace');
    const replaceMarkId2 = Object.entries(suggest2a.marks ?? {}).find(([, m]) => m?.kind === 'replace')?.[0] ?? '';
    assert(replaceMarkId2.length > 0, 'Pass 2 expected replace suggestion id from bridge');

    await waitFor(
      () => client2.isConnectedAndSynced() && client2.getYdoc().getMap('marks').has(replaceMarkId2),
      5_000,
      'pass2 replace suggestion synced to live client2',
    );
    const hydrated2a = hydrateSuggestionMarkInFragment(client2.getYdoc(), 'Alpha', {
      id: replaceMarkId2,
      kind: 'replace',
      by: 'ai:reviewer',
      content: 'Gamma',
    });
    assert(hydrated2a, 'Pass 2 expected to hydrate replace <proofSuggestion>');
    await waitFor(() => client2.getProvider().unsyncedChanges === 0, 5_000, 'pass2 replace hydration flush');
    await sleep(600);

    const accept2aRes = await fetch(`${httpBase}/api/agent/${doc2.slug}/marks/accept`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
        'x-share-token': doc2.ownerSecret,
      },
      body: JSON.stringify({
        markId: replaceMarkId2,
        by: 'human:editor',
      }),
    });
    const accept2aText = await accept2aRes.text();
    const accept2aPayload = JSON.parse(accept2aText) as AcceptResponse;
    assert(
      accept2aRes.status === 200 && accept2aPayload.success === true,
      `Pass 2 Accept 1 failed: HTTP ${accept2aRes.status} code=${String(accept2aPayload.code)} reason=${String(accept2aPayload.reason ?? accept2aPayload.collab?.reason)} body=${accept2aText.slice(0, 300)}`,
    );
    assert(
      accept2aPayload.collab?.status === 'confirmed' && accept2aPayload.collab?.canonicalConfirmed === true,
      `Pass 2 Accept 1 expected confirmed collab status, got ${JSON.stringify(accept2aPayload.collab)}`,
    );

    // Wait for auto-reconnected client to settle after access_epoch bump and flush post-sync attrs
    await waitFor(
      () => client2.getReconnectCount() >= 1
        && client2.isConnectedAndSynced()
        && client2.getYdoc().getText('markdown').toString().includes('Gamma')
        && client2.getProvider().unsyncedChanges === 0,
      10_000,
      'pass2 client2 reconnected and settled after Accept 1',
    );
    await sleep(600);

    const state2aRes = await fetch(`${httpBase}/api/agent/${doc2.slug}/state`, {
      headers: { ...CLIENT_HEADERS, 'x-share-token': doc2.ownerSecret },
    });
    const state2a = await mustJson<StateResponse>(state2aRes, 'pass2 post-accept1 state');
    const markdown2a = typeof state2a.markdown === 'string' ? state2a.markdown : (state2a.content ?? '');
    assert(markdown2a.includes('Gamma') && !markdown2a.includes('Alpha'), `Pass 2 Accept 1 expected Gamma without Alpha, got ${JSON.stringify(markdown2a)}`);
    assert(!state2a.marks?.[replaceMarkId2], 'Pass 2 Accept 1 expected replace suggestion cleared');
    assert(
      Object.values(state2a.marks ?? {}).some((m) => m?.kind === 'authored'),
      'Pass 2 Accept 1 expected authored mark present',
    );
    assert(state2a.readSource === 'projection', `Pass 2 Accept 1 expected readSource=projection, got ${String(state2a.readSource)}`);
    assert(state2a.projectionFresh === true, `Pass 2 Accept 1 expected projectionFresh=true, got ${String(state2a.projectionFresh)}`);

    const suggest2bRes = await fetch(`${httpBase}/documents/${doc2.slug}/bridge/suggestions`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        kind: 'insert',
        quote: 'Gamma',
        content: 'Gamma Inserted',
        by: 'ai:reviewer',
      }),
    });
    const suggest2b = await mustJson<BridgeSuggestResponse>(suggest2bRes, 'pass2 bridge suggest insert');
    const insertMarkId2 = Object.entries(suggest2b.marks ?? {}).find(([, m]) => m?.kind === 'insert')?.[0] ?? '';
    assert(insertMarkId2.length > 0, 'Pass 2 expected insert suggestion id from bridge');

    await waitFor(
      () => client2.isConnectedAndSynced() && client2.getYdoc().getMap('marks').has(insertMarkId2),
      5_000,
      'pass2 insert suggestion synced to reconnected client2',
    );
    const hydrated2b = hydrateSuggestionMarkInFragment(client2.getYdoc(), 'Gamma', {
      id: insertMarkId2,
      kind: 'insert',
      by: 'ai:reviewer',
      content: 'Gamma Inserted',
    });
    assert(hydrated2b, 'Pass 2 expected to hydrate insert <proofSuggestion>');
    await waitFor(() => client2.getProvider().unsyncedChanges === 0, 5_000, 'pass2 insert hydration flush');
    await sleep(600);

    // Simulate real browser reconnect latency (~5.15s) crossing the 5s cooldown
    // while default collab stability verification is still in flight.
    client2.setReconnectDelayMs(5_150);
    const reconnectsBeforeAccept2 = client2.getReconnectCount();
    const accept2bRes = await fetch(`${httpBase}/api/agent/${doc2.slug}/marks/accept`, {
      method: 'POST',
      headers: {
        ...CLIENT_HEADERS,
        'Content-Type': 'application/json',
        'x-share-token': doc2.ownerSecret,
      },
      body: JSON.stringify({
        markId: insertMarkId2,
        by: 'human:editor',
      }),
    });
    const accept2bText = await accept2bRes.text();
    const accept2bPayload = JSON.parse(accept2bText) as AcceptResponse;
    assert(
      accept2bRes.status === 200 && accept2bPayload.success === true,
      `Pass 2 Accept 2 failed: HTTP ${accept2bRes.status} code=${String(accept2bPayload.code)} reason=${String(accept2bPayload.reason ?? accept2bPayload.collab?.reason)} body=${accept2bText.slice(0, 300)}`,
    );
    assert(
      accept2bPayload.collab?.status === 'confirmed' && accept2bPayload.collab?.canonicalConfirmed === true,
      `Pass 2 Accept 2 expected confirmed collab status, got ${JSON.stringify(accept2bPayload.collab)}`,
    );

    client2.setReconnectDelayMs(250);
    await waitFor(
      () => client2.getReconnectCount() > reconnectsBeforeAccept2
        && client2.isConnectedAndSynced()
        && client2.getYdoc().getText('markdown').toString().includes('Gamma Inserted')
        && client2.getProvider().unsyncedChanges === 0,
      10_000,
      'pass2 client2 reconnected and settled after Accept 2',
    );
    client2.reemitStaleMarksAndFragment();
    await waitFor(() => client2.getProvider().unsyncedChanges === 0, 5_000, 'pass2 post-accept2 stale reemit flush');
    await sleep(600);

    const state2bRes = await fetch(`${httpBase}/api/agent/${doc2.slug}/state`, {
      headers: { ...CLIENT_HEADERS, 'x-share-token': doc2.ownerSecret },
    });
    const state2b = await mustJson<StateResponse>(state2bRes, 'pass2 post-accept2 state');
    const markdown2b = typeof state2b.markdown === 'string' ? state2b.markdown : (state2b.content ?? '');
    assert(
      markdown2b.includes('Gamma Inserted') && markdown2b.includes('Beta') && !markdown2b.includes('data-proof="suggestion"'),
      `Pass 2 Accept 2 expected Gamma Inserted and Beta without suggestion span, got ${JSON.stringify(markdown2b)}`,
    );
    assert(!state2b.marks?.[replaceMarkId2], 'Pass 2 expected replace suggestion cleared');
    assert(!state2b.marks?.[insertMarkId2], 'Pass 2 expected insert suggestion cleared');
    assert(
      Object.values(state2b.marks ?? {}).some((m) => m?.kind === 'authored'),
      'Pass 2 Accept 2 expected authored marks present',
    );
    assert(state2b.readSource === 'projection', `Pass 2 Accept 2 expected readSource=projection, got ${String(state2b.readSource)}`);
    assert(state2b.projectionFresh === true, `Pass 2 Accept 2 expected projectionFresh=true, got ${String(state2b.projectionFresh)}`);

    console.log('✓ live marks/accept projection freshness and repeat-accept regression passed');
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

runLiveMarksAcceptProjectionStaleRegressionTest()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error(err instanceof Error ? err.message : String(err));
    process.exit(1);
  });
