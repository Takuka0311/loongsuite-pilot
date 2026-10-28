import { afterEach, describe, expect, it, vi } from 'vitest';
import { fileURLToPath } from 'node:url';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import MigrationReporter, { collectedTests } from '../../../scripts/testing/migration-reporter.mjs';
import { collectCases, inventory } from '../../../scripts/testing/legacy-test-inventory.mjs';

describe('历史用例静态清单', () => {
  const temporaryRoots = [];
  afterEach(() => {
    vi.unstubAllEnvs();
    for (const root of temporaryRoots.splice(0)) rmSync(root, { recursive: true, force: true });
  });
  function reporterFixture() {
    const target = join(process.cwd(), 'target');
    mkdirSync(target, { recursive: true });
    const root = mkdtempSync(join(target, 'migration-reporter-'));
    temporaryRoots.push(root);
    return { root, context: { config: { root } }, output: join(root, 'target', 'report.json') };
  }

  it('区分字符串、注释、helper 与测试注册，且不执行历史代码', () => {
    const result = collectCases('old.test.ts', `
      throw new Error('绝不能执行');
      // it('comment', () => {});
      const sample = "test('string', () => {})";
      function helper() {}
      describe('suite', () => { it('real', () => { helper(); }); });
    `);
    expect(result.cases).toEqual([
      { id: 'L6:C33', title: 'real', suites: ['suite'], parameters: [] },
    ]);
    expect(result.sourceHash).toMatch(/^[0-9a-f]{64}$/);
  });

  it('保留别名、参数表、条件、循环与同名用例的独立位置', () => {
    const result = collectCases('old.test.ts', `
      import { it as check, describe as group } from 'vitest';
      group.each(['a', 'b'])('suite %s', () => {
        check.each([[1], [2]])('same %d', value => {});
        check.skipIf(false)('same', () => {});
        for (const value of [3, 4]) { check('same', () => {}); }
      });
    `);
    expect(result.cases).toHaveLength(3);
    expect(new Set(result.cases.map(item => item.id)).size).toBe(3);
    expect(result.cases.map(item => item.title)).toEqual(['same %d', 'same', 'same']);
    expect(result.cases[0].parameters).toEqual(["group.each(['a', 'b'])", 'check.each([[1], [2]])']);
    expect(result.cases[2].parameters).toEqual(["group.each(['a', 'b'])", '[3, 4]']);
  });

  it('保留模板参数化与动态标题，不擅自按一个场景计数', () => {
    const result = collectCases('old.test.mjs', "it.each`a | b\n${1} | ${2}`('sum $a', () => {}); values.forEach(value => it(`case ${value}`, () => {}));");
    expect(result.cases).toHaveLength(2);
    expect(result.cases[0].parameters[0]).toContain('it.each`a | b');
    expect(result.cases[1].title).toBe('`case ${value}`');
    expect(result.cases[1].parameters).toEqual(['values.forEach']);
  });

  it('报告保留展开后标题、套件、源码摘要和跳过失败状态', () => {
    const filepath = fileURLToPath(import.meta.url);
    const report = collectedTests(process.cwd(), [{ filepath, tasks: [{ type: 'suite', name: 'outer', tasks: [
      { type: 'test', name: 'case 1', result: { state: 'pass' } },
      { type: 'test', name: 'case 2', mode: 'skip' },
      { type: 'test', name: 'case 3', result: { state: 'fail' } },
      { type: 'test', name: 'case 4', mode: 'run' },
    ] }] }]);
    expect(report.map(entry => entry.name)).toEqual(['outer > case 1', 'outer > case 2', 'outer > case 3', 'outer > case 4']);
    expect(report.map(entry => entry.state)).toEqual(['pass', 'skip', 'fail', 'fail']);
    expect(report[0].sourceHash).toMatch(/^[0-9a-f]{64}$/);
    expect(report[0].file).toBe('tests/unit/scripts/migration-inventory.test.mjs');
    expect(() => collectedTests(process.cwd(), [{ filepath: '/outside.test.mjs', tasks: [] }])).toThrow('仓库外');
  });

  it('拒绝缺失或越界的报告路径', () => {
    const { context } = reporterFixture();
    vi.stubEnv('PILOT_MIGRATION_REPORT', '');
    expect(() => new MigrationReporter().onInit(context)).toThrow('需要');
    for (const output of ['report.json', '../outside.json', 'target/../../outside.json', 'target-other/report.json']) {
      vi.stubEnv('PILOT_MIGRATION_REPORT', output);
      expect(() => new MigrationReporter().onInit(context)).toThrow('target');
    }
  });

  it('启动先清空旧报告，全局错误将已通过用例标为失败', () => {
    const { root, context, output } = reporterFixture();
    mkdirSync(join(root, 'target'));
    writeFileSync(output, '[{"state":"pass"}]');
    vi.stubEnv('PILOT_MIGRATION_REPORT', 'target/report.json');
    const reporter = new MigrationReporter();
    reporter.onInit(context);
    expect(JSON.parse(readFileSync(output, 'utf8'))).toEqual([]);
    const filepath = join(root, 'host.test.mjs');
    writeFileSync(filepath, "test('host', () => {});");
    reporter.onFinished([{ filepath, tasks: [
      { type: 'test', name: 'host', result: { state: 'pass' } },
    ] }], [new Error('teardown failed')]);
    const entries = JSON.parse(readFileSync(output, 'utf8'));
    expect(entries).toHaveLength(1);
    expect(entries[0]).toMatchObject({ file: 'host.test.mjs', name: 'host', state: 'fail' });
    expect(entries[0].sourceHash).toMatch(/^[0-9a-f]{64}$/);
    reporter.onFinished();
    expect(JSON.parse(readFileSync(output, 'utf8'))).toEqual([]);
  });

  it('语法错误、未知语言与空清单失败，不把无法识别当完成', () => {
    expect(() => collectCases('bad.test.ts', "it('broken', () => { ")).toThrow('语法无效');
    expect(() => collectCases('test_old.py', 'def test_old(): pass')).toThrow('未支持');
    expect(() => collectCases('empty.test.ts', 'const x = 1;')).toThrow('未识别');
    expect(() => inventory('/unused', '0'.repeat(40))).toThrow('基线不可变更');
  });
});
