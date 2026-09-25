import { copyFile, mkdir, readFile } from 'node:fs/promises';
import sharp from 'sharp';

// The real dashboard owns the source artwork. These are deployable build copies.
const source = new URL('../../crates/admin/admin-ui/img/', import.meta.url);
const output = new URL('../public/brand/', import.meta.url);
await mkdir(output, { recursive: true });
const mark = await readFile(new URL('fibril-mark.svg', source));
await copyFile(new URL('fibril-mark.svg', source), new URL('fibril-mark.svg', output));
for (const size of [16, 32, 48]) {
  await sharp(mark).resize(size, size, { kernel: 'nearest' }).png()
    .toFile(new URL(`fibril-mark-${size}.png`, output).pathname);
}

// Living docs share the dashboard's canonical sprite frames.
await mkdir(new URL('sprites/', output), { recursive: true });
for (const frame of ['open-a', 'open-b', 'open', 'half', 'closed', 'dead']) {
  const name = `ring-${frame}-128.png`;
  await copyFile(new URL(`sprites/${name}`, source), new URL(`sprites/${name}`, output));
}
