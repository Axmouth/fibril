FROM node:22-alpine AS builder

WORKDIR /app/website
ENV ASTRO_TELEMETRY_DISABLED=1
COPY website/package.json website/package-lock.json ./
RUN npm ci
COPY website ./
COPY Cargo.toml /app/Cargo.toml
COPY crates/admin/templates /app/crates/admin/templates
COPY crates/admin/admin-ui /app/crates/admin/admin-ui
COPY crates/admin/tests /app/crates/admin/tests
RUN npm run verify

FROM nginx:1.28-alpine

COPY website/docker/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=builder /app/website/dist /usr/share/nginx/html

EXPOSE 8080
