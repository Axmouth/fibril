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
