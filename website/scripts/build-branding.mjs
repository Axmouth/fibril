import { copyFile, mkdir, readFile, writeFile } from 'node:fs/promises';

// The real dashboard owns the source artwork. These are deployable build copies.
const source = new URL('../../crates/admin/admin-ui/img/', import.meta.url);
const output = new URL('../public/brand/', import.meta.url);
await mkdir(output, { recursive: true });
for (const size of [16, 32, 48]) {
  const name = `face-${size}.png`;
  await copyFile(new URL(name, source), new URL(name, output));
}

// Keep the face artwork canonical; add a vector edge for dark browser tabs.
// Embed the PNG because favicons cannot depend on external SVG subresources.
const face = await readFile(new URL('face-32.png', source));
await writeFile(new URL('face.svg', output), `<svg xmlns="http://www.w3.org/2000/svg" width="32" height="32" viewBox="0 0 32 32">
  <image width="32" height="32" href="data:image/png;base64,${face.toString('base64')}" style="image-rendering:pixelated"/>
  <rect x="0.75" y="0.75" width="30.5" height="30.5" rx="8" fill="none" stroke="#9bd5e9" stroke-opacity="0.8" stroke-width="1.5"/>
</svg>\n`);
