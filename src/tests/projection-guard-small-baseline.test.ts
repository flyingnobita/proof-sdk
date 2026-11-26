import * as Y from 'yjs';
import { evaluateProjectionSafety } from '../../server/collab.js';

function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(message);
}

function run(): void {
  const doc = new Y.Doc();
  const baseline = '# Draft\n\nGood';
  const accepted = '# Draft\n\n<span data-proof="authored" data-by="ai:reviewer" data-id="d1a07511-51d2-4a67-b2b5-2d23c9a36851">Bad!</span>\n';

  // A short document grows more than 8x once accept writes authored markup.
  assert(
    accepted.length > baseline.length * 8,
    'fixture must exceed the default growth multiplier to exercise the guard',
  );

  const allowed = evaluateProjectionSafety(baseline, accepted, doc);
  assert(
    allowed.safe,
    `short-document suggestion markup should pass the projection guard, got ${allowed.reason ?? 'unknown'}`,
  );

  // The bypass must not disable pathological-repeat protection.
  const repeated = evaluateProjectionSafety(accepted, `${accepted}${accepted}`, doc);
  assert(
    !repeated.safe && repeated.reason === 'pathological_repeat',
    `repeated projection content should still be blocked, got ${repeated.reason ?? 'safe'}`,
  );

  // Larger documents keep the growth ratio guard instead of the small-baseline bypass.
  const largeBaseline = `# Draft\n\n${'word '.repeat(40)}`;
  const huge = `# Draft\n\n${'different words here '.repeat(400)}`;
  const blocked = evaluateProjectionSafety(largeBaseline, huge, doc);
  assert(
    !blocked.safe,
    'large-document projections should still be refused when growth and overlap guards trip',
  );

  console.log('projection-guard-small-baseline: 4 assertions passed');
}

run();
