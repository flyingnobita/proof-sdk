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
  const shareBranch = block.split('    let success = false;')[0];
  assert(shareBranch.length > 0 && shareBranch.length < block.length, 'Expected the share-mode branch to precede the local accept path');

  assert(
    shareBranch.includes('applied = acceptMark(view, markId, parser);'),
    'Expected share-mode markAccept to apply the suggestion locally',
  );
  assert(
    !shareBranch.includes('shareClient.acceptSuggestion(markId, actor)'),
    'Expected share-mode markAccept to skip the redundant accept mutation after the local apply',
  );

  console.log('share-accept-local-apply: assertions passed');
}

run();
