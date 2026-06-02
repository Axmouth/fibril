FROM node:22-alpine AS builder

WORKDIR /app
ENV ASTRO_TELEMETRY_DISABLED=1
COPY website/package.json website/package-lock.json ./
RUN npm ci
COPY website ./
RUN npm run build

FROM nginx:1.28-alpine

COPY website/docker/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=builder /app/dist /usr/share/nginx/html

EXPOSE 8080
