import { build } from 'esbuild'
import { createHash } from 'node:crypto'
import { mkdir, readFile, writeFile } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import { buildProtocol } from './build-protocol.mjs'
import { writeArtifact } from './artifacts.mjs'

const root = new URL('../', import.meta.url)
await buildProtocol()
const host = await build({
  absWorkingDir: fileURLToPath(root), entryPoints: ['index.js'], bundle: true, write: false,
  platform: 'node', format: 'esm', target: 'node22.19', packages: 'external', legalComments: 'inline',
})
const text = host.outputFiles[0].text
const revision = createHash('sha256').update(text).digest('hex').slice(0, 12)
const directory = new URL(`dist/host/${revision}/`, root)
await mkdir(directory, { recursive: true })
const path = new URL('index.js', directory)
let previous
try { previous = await readFile(path, 'utf8') } catch (error) { if (error.code !== 'ENOENT') throw error }
if (previous !== undefined && previous !== text) throw new Error(`Immutable host release collision ${revision}`)
if (previous === undefined) await writeFile(path, text, { flag: 'wx' })
const client = await build({
  absWorkingDir: fileURLToPath(root), entryPoints: ['client-entry.js'], outfile: 'lib/client.js', bundle: true, write: false,
  platform: 'browser', format: 'iife', target: 'es2022', legalComments: 'inline',
})
await writeArtifact('lib/client.js', client.outputFiles[0].contents)
const patch = `- insert:\n    - id: cursor\n      name: ./dist/host/${revision}/index.js\n`
const patchPath = new URL('cordis.patch.yml', root)
if (await readFile(patchPath, 'utf8') !== patch) await writeArtifact('cordis.patch.yml', patch)
console.log(`Cursor native provider ${revision}`)
