import { createRequire } from 'node:module'

export function productCopy(text) {
  // Protocols, URLs, package ids, model names and filesystem identifiers are
  // not product copy. Rebranding those would break the retained integrations.
  if (/https?:\/\/|X-Hermes-|@hermes\/|\bHermes[- :]?[234]\b|copyright|all rights reserved/i.test(text)) return text
  if (!/\s/.test(text) && text !== 'Hermes' && text !== 'HERMES') return text
  return text.replace(/\b(?:Hermes Agent|HERMES AGENT)\b/g, 'Stumpy').replace(/\b(?:Hermes|HERMES)\b/g, 'GameDevBuddy AI')
}

/** Brand displayed text at the product boundary. Identifiers, imports, licence
 * notices, provider protocols and model names keep their original identities. */
export function gameDevBuddyBranding(workspace) {
  const require = createRequire(`${workspace}/apps/desktop/package.json`)
  const ts = require('typescript')
  return {
    name: 'gamedevbuddy-product-text', enforce: 'pre',
    transform(code, id) {
      if (/license|copyright|legal/i.test(id)) return null
      if (/\.test\.[jt]sx?$|\/src\/test\//.test(id)) return null
      if (!id.includes('/src/') || !/\.[jt]sx?$/.test(id) || id.includes('/node_modules/')) return null
      const source = ts.createSourceFile(id, code, ts.ScriptTarget.Latest, true, id.endsWith('x') ? ts.ScriptKind.TSX : ts.ScriptKind.TS)
      const edits = []
      const visit = node => {
        if ((ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) && productCopy(node.text) !== node.text) {
          // Import/export specifiers and object keys are protocol/source, not UI.
          const parent = node.parent
          if (!ts.isImportDeclaration(parent) && !ts.isExportDeclaration(parent) &&
              !(ts.isPropertyAssignment(parent) && parent.name === node)) {
            edits.push([node.getStart(source), node.end, JSON.stringify(productCopy(node.text))])
          }
        } else if (ts.isJsxText(node) && productCopy(node.text) !== node.text) {
          edits.push([node.pos, node.end, productCopy(node.text)])
        }
        ts.forEachChild(node, visit)
      }
      visit(source)
      for (const [start,end,value] of edits.sort((a,b) => b[0]-a[0])) code = code.slice(0,start)+value+code.slice(end)
      return edits.length ? {code,map:null} : null
    }
  }
}
