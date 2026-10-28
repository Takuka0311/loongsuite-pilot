// 只解析历史源码，不导入或执行其中的测试、配置和 helper。
import ts from 'typescript';
import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { pathToFileURL } from 'node:url';

export const BASELINE = 'ef8953de1b5d2aa6d719cb0409cb5f48f2b99b42';
const testNames = new Set(['it', 'test']);
const suiteNames = new Set(['describe', 'suite']);

function rootName(expression) {
  if (ts.isIdentifier(expression)) return expression.text;
  if (ts.isPropertyAccessExpression(expression) || ts.isElementAccessExpression(expression)) {
    return rootName(expression.expression);
  }
  if (ts.isCallExpression(expression)) return rootName(expression.expression);
  if (ts.isTaggedTemplateExpression(expression)) return rootName(expression.tag);
  return '';
}

function titleOf(node, source) {
  if (!node) return '<missing-title>';
  return ts.isStringLiteralLike(node) ? node.text : node.getText(source);
}

export function collectCases(path, text) {
  if (!/\.(?:[cm]?js|tsx?)$/.test(path)) {
    throw new Error(`未支持的历史测试语言：${path}`);
  }
  const source = ts.createSourceFile(path, text, ts.ScriptTarget.Latest, true);
  if (source.parseDiagnostics.length) throw new Error(`历史测试语法无效：${path}`);
  const aliases = new Map();
  for (const statement of source.statements) {
    if (!ts.isImportDeclaration(statement) || statement.moduleSpecifier.text !== 'vitest') continue;
    const bindings = statement.importClause?.namedBindings;
    if (!bindings || !ts.isNamedImports(bindings)) continue;
    for (const specifier of bindings.elements) {
      aliases.set(specifier.name.text, (specifier.propertyName ?? specifier.name).text);
    }
  }
  const cases = [];
  function walk(node, suites = [], contexts = []) {
    if (ts.isCallExpression(node)) {
      const root = rootName(node.expression);
      const name = aliases.get(root) ?? root;
      // each([...])、skipIf(...) 等构造调用不是测试注册；只收最外层调用。
      const chained = (ts.isCallExpression(node.parent) && node.parent.expression === node)
        || ((ts.isPropertyAccessExpression(node.parent) || ts.isElementAccessExpression(node.parent))
          && node.parent.expression === node);
      if (!chained && (testNames.has(name) || suiteNames.has(name))) {
        const title = titleOf(node.arguments[0], source);
        const registration = node.expression.getText(source);
        if (testNames.has(name)) {
          const { line, character } = source.getLineAndCharacterOfPosition(node.getStart(source));
          cases.push({
            id: `L${line + 1}:C${character + 1}`,
            title,
            suites,
            parameters: [...contexts, ...(registration === root ? [] : [registration])],
          });
          return;
        }
        for (const argument of node.arguments.slice(1)) {
          walk(argument, [...suites, title], [...contexts, ...(registration === root ? [] : [registration])]);
        }
        return;
      }
    }
    let nested = contexts;
    if (ts.isForOfStatement(node) || ts.isForInStatement(node)) {
      nested = [...contexts, node.expression.getText(source)];
    } else if (ts.isForStatement(node)) {
      nested = [...contexts, [node.initializer, node.condition, node.incrementor]
        .filter(Boolean).map(part => part.getText(source)).join('; ')];
    } else if (ts.isCallExpression(node) && ts.isPropertyAccessExpression(node.expression)
      && ['forEach', 'map'].includes(node.expression.name.text)) {
      nested = [...contexts, node.expression.getText(source)];
    }
    ts.forEachChild(node, child => walk(child, suites, nested));
  }
  walk(source);
  if (!cases.length) throw new Error(`历史文件未识别到测试，需显式扩展解析规则：${path}`);
  return { path, sourceHash: createHash('sha256').update(text).digest('hex'), cases };
}

export function inventory(root, baseline = BASELINE) {
  if (baseline !== BASELINE) throw new Error('历史测试基线不可变更');
  const git = (...args) => execFileSync('git', args, {
    cwd: root, encoding: 'utf8', maxBuffer: 32 * 1024 * 1024,
    timeout: 30_000, stdio: ['ignore', 'pipe', 'pipe'],
  });
  const paths = git('ls-tree', '-rz', '--name-only', baseline, '--', 'tests')
    .split('\0').filter(path => /(?:\.test\.|\.spec\.|\/test[_-]|Tests\.swift$)/.test(path)
      && /\.(?:tsx?|[cm]?js|py|sh|ps1|swift)$/.test(path));
  if (!paths.length) throw new Error('历史测试清单为空');
  return paths.sort().map(path => collectCases(path, git('show', `${baseline}:${path}`)));
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try {
    const [root, baseline = BASELINE] = process.argv.slice(2);
    if (!root) throw new Error('需要仓库路径');
    process.stdout.write(JSON.stringify(inventory(root, baseline)) + '\n');
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
