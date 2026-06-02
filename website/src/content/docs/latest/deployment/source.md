---
title: Deployment from source
description: Current development deployment guidance for Fibril.
---

The broker does not have a production-ready packaged deployment yet. Run it from source while the configuration surface is being built.

```sh
cargo run --release --bin fibril-server
```

The server binary currently uses development defaults in source for:

- bind addresses
- data directory
- broker authentication
- metrics behavior

Do not expose it publicly without reviewing those defaults and placing it behind appropriate network controls.

## Website deployment

The documentation website is separate from the broker process. It builds to static files and is served by nginx in a small container behind Traefik.

```sh
cd website
npm ci
npm run build
```
