// Build the full first-party client as a directly mounted module. Never changes
// the native Desktop install or strips contributions, models, tools or skills.
import { createRequire } from 'node:module'
import { mkdirSync, readFileSync, writeFileSync, copyFileSync, readdirSync } from 'node:fs'
import { execFileSync } from 'node:child_process'
import { join, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import { createHash } from 'node:crypto'
import { mirrorWebappInputs } from './build-webapp.mjs'
import { repoRoot, workspaceTool } from '../../../scripts/build/frontend-common.mjs'
import { prepareNodeDependencies } from '../../../scripts/build/node-deps.mjs'

const workspace = resolve(repoRoot, '.build/webapp-workspace')
const out = resolve(repoRoot, 'apps/desktop/dist-gamedevbuddy')
mkdirSync(workspace, { recursive: true })
mirrorWebappInputs(repoRoot, workspace)
// Renderer typechecking also imports this unchanged shared contract fixture.
mkdirSync(join(workspace, 'tests/fixtures'), {recursive:true})
copyFileSync(join(repoRoot,'tests/fixtures/session-resume-active-turn.json'),join(workspace,'tests/fixtures/session-resume-active-turn.json'))
prepareNodeDependencies({source: workspace, workspaces: ['apps/desktop'], reuse: true, install: !process.argv.includes('--no-install'),
  env: {...process.env, npm_config_ignore_scripts: 'true'}})
const { build } = await import(pathToFileURL(workspaceTool(workspace, 'apps/desktop', 'vite')).href)
await build({
  root: join(workspace, 'apps/desktop'), configLoader: 'runner',
  define: { 'process.env.NODE_ENV': JSON.stringify('production'), 'process.env': '{}' },
  build: {
    outDir: out, emptyOutDir: true, cssCodeSplit: false,
    lib: {entry: join(workspace, 'apps/desktop/src/gamedevbuddy/workspace.tsx'), formats: ['es'], fileName: () => 'workspace.js', cssFileName: 'workspace'},
  }
})
// Scope every emitted stylesheet, including full editor/terminal/diagram styles.
// @keyframes/font declarations remain declarations, not prefixed selectors.
const require = createRequire(join(workspace, 'apps/desktop/package.json'))
const postcss = require('postcss')
const selectorParser = require('postcss-selector-parser')
const cssPath = join(out, 'workspace.css')
const css = postcss.parse(readFileSync(cssPath, 'utf8'))
css.walkRules(rule => {
  if (rule.parent?.type === 'atrule' && /keyframes$/i.test(rule.parent.name)) return
  rule.selector = selectorParser(selectors => {
    selectors.each(selector => {
      const scope = ':is([data-gdb-ai-root][data-gdb-ai-root])'
      let rootSelector = false
      selector.walk(node => {
        if ((node.type === 'tag' && ['html', 'body'].includes(node.value)) ||
            (node.type === 'id' && node.value === 'root') ||
            (node.type === 'pseudo' && node.value === ':root')) {
          node.replaceWith(selectorParser().astSync(scope).first.first.clone())
          rootSelector = true
        }
      })
      if (!rootSelector) {
        selector.prepend(selectorParser.combinator({value:' '}))
        selector.prepend(selectorParser().astSync(scope).first.first.clone())
      }
    })
  }).processSync(rule.selector)
})
writeFileSync(cssPath, css.toString().replace(/url\(\s*(["']?)\/(fonts|ds-assets)\//g, 'url($1./$2/'))
const revision = createHash('sha256').update(readFileSync(join(out,'workspace.js'))).digest('hex')
const sourceRevision = execFileSync('git',['rev-parse','HEAD'],{cwd:repoRoot,encoding:'utf8'}).trim()
const sourceDirty = Boolean(execFileSync('git',['status','--porcelain'],{cwd:repoRoot,encoding:'utf8'}).trim())
const files = {}
function inventory(folder, prefix='') {
  for (const entry of readdirSync(folder,{withFileTypes:true})) {
    const name = prefix + entry.name
    if (entry.isSymbolicLink()) throw new Error('Client build contains a symbolic link')
    if (entry.isDirectory()) inventory(join(folder,entry.name),name+'/')
    else if (name !== 'gamedevbuddy-client.json') files[name] = createHash('sha256').update(readFileSync(join(folder,entry.name))).digest('hex')
  }
}
inventory(out)
writeFileSync(join(out, 'gamedevbuddy-client.json'), JSON.stringify({schema:1, product:'GameDevBuddy AI', agent:'Stumpy', revision, sourceRevision, sourceDirty, files, entry:'workspace.js', css:'workspace.css', fullClient:true},null,2)+'\n')
console.log(`GameDevBuddy AI full client: ${out}`)
