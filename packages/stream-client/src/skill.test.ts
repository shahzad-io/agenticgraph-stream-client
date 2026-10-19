import { readFileSync } from 'node:fs';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { describe, expect, it } from 'vitest';

/*
 * Drift test for skills/agenticgraph-stream-client/SKILL.md. It fails when the skill names an export, a method,
 * a constructor option, a stop reason, a status, an error kind or the IIFE global that the package no longer has.
 */

const pkg = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const read = (path: string): string => readFileSync(resolve(pkg, path), 'utf8');
const skill = read('skills/agenticgraph-stream-client/SKILL.md');

const CLASS_FILES: Record<string, string[]> = {
  stream: ['src/agentic-stream.ts'],
  AgenticStream: ['src/agentic-stream.ts'],
  transport: [
    'src/transports/sse-transport.ts',
    'src/transports/echo-transport.ts',
    'src/transports/mock-transport.ts',
  ],
  SSETransport: ['src/transports/sse-transport.ts'],
  EchoTransport: ['src/transports/echo-transport.ts'],
  MockTransport: ['src/transports/mock-transport.ts'],
};

/** `receiver.method(` calls whose method the receiver's class does not declare. */
function missingCalls(text: string): string[] {
  const missing: string[] = [];

  for (const [, receiver, method] of text.matchAll(
    /\b(stream|AgenticStream|transport|SSETransport|EchoTransport|MockTransport)\.(\w+)\s*\(/g,
  )) {
    const declaration = new RegExp(`^\\s+(?:static\\s+|async\\s+|private\\s+|readonly\\s+)*${method}\\s*\\(`, 'm');
    const declared = CLASS_FILES[receiver].some((file) => declaration.test(read(file)));

    if (!declared) {
      missing.push(`${receiver}.${method}`);
    }
  }

  return missing;
}

/** Top-level keys of every object literal that follows `open` (a regex source ending in `\{`). */
function objectKeys(text: string, open: string): string[] {
  const keys: string[] = [];

  for (const match of text.matchAll(new RegExp(open, 'g'))) {
    let depth = 1;
    let expectKey = true;

    for (let i = (match.index ?? 0) + match[0].length; i < text.length && depth > 0; i += 1) {
      const ch = text[i];

      if (depth === 1 && expectKey && /\w/.test(ch)) {
        const key = /^\w+/.exec(text.slice(i))?.[0] ?? '';
        keys.push(key);
        expectKey = false;
        i += key.length - 1;

        continue;
      }

      if (depth === 1 && ch === ',') {
        expectKey = true;
      }

      if ('{(['.includes(ch)) {
        depth += 1;
      } else if ('})]'.includes(ch)) {
        depth -= 1;
      }
    }
  }

  return keys;
}

/** Property names of `export interface NAME { ... }` in a source file. */
function interfaceKeys(file: string, name: string): string[] {
  const source = read(file);
  const body = new RegExp(`export interface ${name} \\{([\\s\\S]*?)\\n\\}`).exec(source)?.[1] ?? '';

  return [...body.matchAll(/^\s+(\w+)\??:/gm)].map((m) => m[1]);
}

/** Members of `export type NAME = 'a' | 'b';` in a source file. */
function unionMembers(file: string, name: string): string[] {
  const source = read(file);
  const body = new RegExp(`export type ${name} =([\\s\\S]*?);`).exec(source)?.[1] ?? '';

  return [...body.matchAll(/'(\w+)'/g)].map((m) => m[1]);
}

const exported = new Set(
  [...read('src/index.ts').matchAll(/export\s+(?:type\s+)?\{([^}]*)\}/g)].flatMap((m) =>
    m[1]
      .split(',')
      .map((s) => s.trim().split(/\s+as\s+/).pop() ?? '')
      .filter((s) => s !== ''),
  ),
);

describe('agenticgraph-stream-client skill', () => {
  it('imports only names the package exports', () => {
    const imported = [...skill.matchAll(/import\s+(?:type\s+)?\{([^}]*)\}\s+from\s+'@agenticgraph\/stream-client'/g)]
      .flatMap((m) => m[1].split(',').map((s) => s.trim().replace(/^type\s+/, '')))
      .filter((s) => s !== '');

    expect(imported.length).toBeGreaterThan(0);
    expect(imported.filter((name) => !exported.has(name))).toEqual([]);
  });

  it('calls only methods the classes declare', () => {
    expect(missingCalls(skill)).toEqual([]);
  });

  it('passes only options the constructors take', () => {
    const sse = interfaceKeys('src/transports/sse-types.ts', 'SSETransportOptions');
    const echo = interfaceKeys('src/transports/echo-types.ts', 'EchoTransportOptions');
    const connect = interfaceKeys('src/agentic-stream.ts', 'AgenticStreamConnectOptions');

    expect(objectKeys(skill, 'new SSETransport\\(\\{')).toContain('resolveUrl');
    expect(objectKeys(skill, 'new SSETransport\\(\\{').filter((k) => !sse.includes(k))).toEqual([]);
    expect(objectKeys(skill, 'new EchoTransport\\(\\{').filter((k) => !echo.includes(k))).toEqual([]);
    expect(objectKeys(skill, 'AgenticStream\\.connect\\(\\{').filter((k) => !connect.includes(k))).toEqual([]);
  });

  it('names every stop reason, status and error kind the package has', () => {
    const names = [
      ...unionMembers('src/transport.ts', 'StreamTransportStopReason'),
      ...unionMembers('src/types/status.ts', 'StreamStatus'),
      ...[...read('src/stream/stream-error.ts').matchAll(/kind: '(\w+)'/g)].map((m) => m[1]),
    ];

    expect(names.length).toBeGreaterThan(10);
    expect(names.filter((name) => !skill.includes(`\`${name}\``))).toEqual([]);
  });

  it('uses the IIFE global name and file that the build produces', () => {
    const global = /globalName: '(\w+)'/.exec(read('tsup.config.ts'))?.[1];
    const pkgJson = JSON.parse(read('package.json')) as { exports: Record<string, string> };

    expect(global).toBe('AgenticGraphStream');
    expect(pkgJson.exports['./iife']).toBe('./dist/index.global.js');
    expect(skill).toContain(`window.${global}`);
    expect(skill).toContain('dist/index.global.js');
  });

  it('ships with the package and is linked from its README', () => {
    const pkgJson = JSON.parse(read('package.json')) as { files: string[] };
    const frontmatter = /^---\nname: ([\w-]+)\ndescription: .+\n---\n/.exec(skill);

    expect(pkgJson.files).toContain('skills');
    expect(frontmatter?.[1]).toBe('agenticgraph-stream-client');
    expect(read('README.md')).toContain('skills/agenticgraph-stream-client/SKILL.md');
  });

  it('catches a renamed method and a renamed option', () => {
    expect(missingCalls('stream.getStopReasonRenamed(); transport.simulateDrop();')).toEqual([
      'stream.getStopReasonRenamed',
    ]);
    expect(objectKeys("new SSETransport({ url: u, resolveUrlRenamed: f })", 'new SSETransport\\(\\{')).toEqual([
      'url',
      'resolveUrlRenamed',
    ]);
  });
});
