import { build } from 'esbuild'
import { fileURLToPath } from 'node:url'
import { resolve } from 'node:path'
import { writeArtifact } from './artifacts.mjs'

const root = fileURLToPath(new URL('../', import.meta.url))

export async function buildProtocol() {
  const result = await build({
    absWorkingDir: root,
    entryPoints: ['src/protocol-entry.ts'],
    outfile: 'lib/protocol.js',
    write: false,
    bundle: true,
    platform: 'node',
    target: 'node22.19',
    format: 'esm',
    external: ['@deepseek-ai/*', '@bufbuild/protobuf', '@bufbuild/protobuf/*'],
    legalComments: 'inline',
    banner: { js: '/* dsh-cursor in-process wire core. MIT upstream notices in UPSTREAM.md. */' },
  })
  await writeArtifact('lib/protocol.js', result.outputFiles[0].contents)
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) await buildProtocol()
