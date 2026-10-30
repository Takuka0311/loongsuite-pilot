// Validate source provenance separately from execution and the frozen 1.x baseline.
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import { collectCases } from './legacy-test-inventory.mjs';

function relativeFile(path) {
  assert.equal(typeof path, 'string');
  assert(!path.startsWith('/') && !path.includes('\\') && !path.split('/').some(p => !p || p === '.' || p === '..'));
  return path;
}
const manifest = JSON.parse(readFileSync(process.argv[2] ?? 'tests/fixtures/rust-migration-main-map.json', 'utf8'));
assert.equal(manifest.schema, 2);
assert.match(manifest.baselineCommit, /^[0-9a-f]{40}$/);
assert(Array.isArray(manifest.mappings));
let count = 0;
const files = new Set();
for (const mapping of manifest.mappings) {
  const path = relativeFile(mapping.legacy);
  assert(path.startsWith('tests/'));
  assert(!files.has(path), 'duplicate source file');
  files.add(path);
  const bytes = execFileSync('git', ['show', manifest.baselineCommit + ':' + path], { maxBuffer: 32 * 1024 * 1024 });
  assert.equal(createHash('sha256').update(bytes).digest('hex'), mapping.source_hash, 'source hash: ' + path);
  const { cases } = collectCases(path, bytes.toString('utf8'));
  const indexed = new Map(cases.map(c => [c.id, c]));
  const seen = new Set();
  for (const item of mapping.cases) {
    assert(!seen.has(item.source.id), 'duplicate declaration');
    seen.add(item.source.id);
    assert.deepEqual(item.source, indexed.get(item.source.id), 'source declaration: ' + path + ':' + item.source.id);
    if (item.target.kind === 'rust') {
      assert(item.target.tests.length > 0);
      for (const target of item.target.tests) {
        const file = relativeFile(target.file);
        assert(/^(src|tests|xtask)\//.test(file), 'native test source outside checkout');
        const name = target.test.split('::').at(-1);
        assert.match(name, /^[A-Za-z_][A-Za-z0-9_]*$/);
        assert(new RegExp('\\bfn\\s+' + name + '\\s*\\(').test(readFileSync(file, 'utf8')), 'missing native test: ' + file + '::' + name);
      }
    }
    count++;
  }
  if (mapping.coverage === 'complete') assert.equal(seen.size, indexed.size, 'incomplete source declaration inventory: ' + path);
}
console.log(JSON.stringify({ baseline: manifest.baselineCommit, files: files.size, declarations: count, sourceProvenanceValid: true, executionVerifiedByThisTool: false }));
