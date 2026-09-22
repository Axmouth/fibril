import { copyFile, mkdir } from 'node:fs/promises';

// The real dashboard owns the source artwork. These are deployable build copies.
const source = new URL('../../crates/admin/admin-ui/img/', import.meta.url);
const output = new URL('../public/brand/', import.meta.url);
await mkdir(output, { recursive: true });
for (const size of [16, 32, 48]) {
  const name = `face-${size}.png`;
  await copyFile(new URL(name, source), new URL(name, output));
}
