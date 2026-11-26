import { readFileSync } from 'node:fs';
import path from 'node:path';

function assert(condition: boolean, message: string): void {
  if (!condition) throw new Error(message);
}

function run(): void {
  const source = readFileSync(path.resolve(process.cwd(), 'src/editor/index.ts'), 'utf8');
  const start = source.indexOf('  markAccept(markId: string): boolean {');
  assert(start !== -1, 'Expected markAccept in the editor source');
  const block = source.slice(start, source.indexOf('\n  /**', start));

  const localApplyIndex = block.indexOf('applied = acceptMark(view, markId, parser);');
  const shareMutationIndex = block.indexOf('void shareClient.acceptSuggestion(markId, actor)');
  assert(localApplyIndex !== -1, 'Expected share-mode markAccept to apply the suggestion locally');
  assert(shareMutationIndex !== -1, 'Expected share-mode markAccept to persist through the share mutation');
  assert(
    localApplyIndex < shareMutationIndex,
    'Expected the local apply to run before the share mutation so the client cannot push pre-accept content over it',
  );

  console.log('share-accept-local-apply: assertions passed');
}

run();
