# Fibril website

The public Fibril site is an Astro + Starlight static build.

## Local development

```sh
npm ci
npm run dev
```

Build the deployable static output with:

```sh
ASTRO_TELEMETRY_DISABLED=1 npm run build
```

## Deployment

`../.github/workflows/deploy-website.yaml` builds `website/Dockerfile`, publishes the image to GHCR, copies `../compose.website.deploy.yaml` to the VPS, and starts it behind the external Traefik `web` network.

Configure the GitHub `Deploy` environment with:

- secrets: `DEPLOY_SSH_HOST`, `DEPLOY_SSH_KEY`, `DEPLOY_SSH_PORT`, `DEPLOY_SSH_USER`, `DEPLOY_KNOWN_HOSTS`
- variables: `DEPLOY_PATH`, `DEPLOY_HEALTH_URL`

Create a `.env` file in `DEPLOY_PATH` based on `../.env.website.example`. The VPS must already have an external Docker network named `web` connected to Traefik.

## Shared dashboard and mascot sources

The website builds from the repository checkout. Dashboard demo templates and
assets come from `../crates/admin/`; see `demo/README.md`. The Docker build preserves
that same directory layout and includes those sources.

`BrandMascot.astro` imports the dashboard's open/closed ring sprites directly.
The landing page and docs header share that component: a 160ms blink every eight
seconds, with a still image when reduced motion is preferred. Favicon images use
the existing 16/32/48px face crops so the mascot stays legible at tab size.
`npm run assets:build` copies those canonical face files into ignored, generated
`public/brand/`; both `dev` and `build` run it. Edit artwork in
`crates/admin/admin-ui/img`, not the generated files. A fresh build propagates
asset changes to the website and demo without maintaining another authored copy.
