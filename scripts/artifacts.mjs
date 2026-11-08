import { randomUUID } from 'node:crypto'
import { writeFile, rename, unlink } from 'node:fs/promises'
import { resolve, dirname, join, sep } from 'node:path'
import { fileURLToPath } from 'node:url'

const root = fileURLToPath(new URL('../', import.meta.url))

/** Replace generated files atomically so a pnpm hardlink cannot mutate an installed copy. */
export async function writeArtifact(relativePath, contents) {
  const target = resolve(root, relativePath)
  if (!target.startsWith(resolve(root) + sep)) throw new Error('Artifact target must stay inside the project')
  const temporary = join(dirname(target), `.cursor-build-${randomUUID()}.tmp`)
  // Both absolute paths are confined to the intended artifact's project directory.
  await writeFile(temporary, contents, { flag: 'wx', mode: 0o600 })
  try { await rename(temporary, target) }
  finally {
    try { await unlink(temporary) } catch (error) { if (error.code !== 'ENOENT') throw error }
  }
}
