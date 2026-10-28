// Vitest 1.x 的实际收集与执行报告；文件摘要用于拒绝修改前的过期证据。
import { createHash } from 'node:crypto';
import { mkdirSync, readFileSync, renameSync, writeFileSync } from 'node:fs';
import { dirname, isAbsolute, relative, resolve, sep } from 'node:path';

export function collectedTests(root, files) {
  const entries = [];
  for (const file of files) {
    const path = relative(root, file.filepath);
    if (!path || isAbsolute(path) || path.split(sep).includes('..')) {
      throw new Error('收集到仓库外测试');
    }
    const sourceHash = createHash('sha256').update(readFileSync(file.filepath)).digest('hex');
    function walk(tasks, suites) {
      for (const task of tasks ?? []) {
        if (task.type === 'suite') {
          walk(task.tasks, [...suites, task.name]);
        } else if (task.type === 'test') {
          const state = task.result?.state ?? task.mode;
          entries.push({ file: path.split(sep).join('/'), sourceHash,
            platform: ({ linux: 'linux', darwin: 'macos', win32: 'windows' })[process.platform],
            name: [...suites, task.name].join(' > '),
            state: ['pass', 'fail', 'skip', 'todo'].includes(state) ? state : 'fail' });
        }
      }
    }
    walk(file.tasks, []);
  }
  return entries;
}

export default class MigrationReporter {
  onInit(context) {
    this.root = context.config.root;
    if (!process.env.PILOT_MIGRATION_REPORT) throw new Error('需要 PILOT_MIGRATION_REPORT 输出路径');
    this.output = resolve(this.root, process.env.PILOT_MIGRATION_REPORT);
    const path = relative(this.root, this.output);
    if (isAbsolute(path) || !path.startsWith(`target${sep}`) || path.split(sep).includes('..')) {
      throw new Error('迁移报告必须写入仓库 target 目录');
    }
    mkdirSync(dirname(this.output), { recursive: true });
    writeFileSync(this.output, '[]\n');
  }

  onFinished(files = [], errors = []) {
    const entries = collectedTests(this.root, files);
    // 全局 setup/teardown 失败时也不能留下全通过证据。
    if (errors.length) for (const entry of entries) entry.state = 'fail';
    mkdirSync(dirname(this.output), { recursive: true });
    const temporary = `${this.output}.${process.pid}.tmp`;
    writeFileSync(temporary, JSON.stringify(entries, null, 2) + '\n');
    renameSync(temporary, this.output);
  }
}
