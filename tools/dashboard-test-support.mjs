import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { Script, createContext } from 'node:vm';

// Evaluate exact named functions from the shipped page. The override allows
// the same controls to reproduce defects against an archived original page.
export const html = readFileSync(process.env.DASHBOARD_SOURCE ||
  new URL('../crates/logex-server/src/web_ui.html', import.meta.url), 'utf8');

export function functionSource(name) {
  const start = html.indexOf(`\nfunction ${name}(`);
  assert.notEqual(start, -1, `Missing dashboard function ${name}`);
  let candidate = '';
  for (const line of html.slice(start + 1).split('\n')) {
    candidate += `${line}\n`;
    try {
      new Script(`(${candidate})`);
      return candidate;
    } catch (error) {
      if (!(error instanceof SyntaxError)) throw error;
    }
  }
  throw new Error(`Cannot extract ${name}`);
}

export function dashboard(names, globals = {}) {
  const context = createContext(globals);
  for (const name of names) new Script(functionSource(name)).runInContext(context);
  return context;
}
