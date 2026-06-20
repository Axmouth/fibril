import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  site: "https://fibril.sh",
  integrations: [
    starlight({
      title: "Fibril",
      description: "A lightweight message broker with durable queues and explicit delivery semantics.",
      logo: {
        src: "./src/assets/fibril-mark.svg",
      },
      customCss: ["./src/styles/starlight.css"],
      favicon: "/favicon.svg",
      social: [
        { icon: "github", label: "GitHub", href: "https://github.com/Axmouth/fibril" },
      ],
      sidebar: [
        {
          label: "Start Here",
          items: [
            { label: "Overview", slug: "latest" },
            { label: "Quickstart", slug: "latest/quickstart" },
            { label: "Client usage", slug: "latest/clients" },
            { label: "Configuration", slug: "latest/configuration" },
            { label: "Project status", slug: "latest/status" },
            { label: "Implemented surface", slug: "latest/implemented-surface" },
          ],
        },
        {
          label: "Concepts",
          items: [
            { label: "Core model", slug: "latest/concepts/core-model" },
            { label: "Consumer groups", slug: "latest/concepts/consumer-groups" },
            { label: "Clustering and coordination", slug: "latest/concepts/clustering" },
            { label: "Reliability semantics", slug: "latest/reliability/semantics" },
            { label: "Retries and delays", slug: "latest/reliability/retries-delays" },
            { label: "Dead lettering", slug: "latest/reliability/dead-lettering" },
            { label: "Replication", slug: "latest/reliability/replication" },
            { label: "Recovery quarantine", slug: "latest/reliability/recovery-quarantine" },
            { label: "Backpressure", slug: "latest/concepts/backpressure" },
            { label: "Many idle queues", slug: "latest/concepts/many-idle-queues" },
            { label: "Benchmarks", slug: "latest/benchmarks" },
            { label: "Roadmap", slug: "latest/roadmap" },
            { label: "Deployment", slug: "latest/deployment/source" },
          ],
        },
        {
          label: "Development Notes",
          items: [
            { label: "Documentation style", slug: "latest/development/docs-writing" },
            { label: "Configuration policy", slug: "latest/development/config-policy" },
            { label: "Configuration design", slug: "latest/development/config-design" },
            { label: "Metadata policy", slug: "latest/development/metadata-policy" },
            { label: "Partition routing", slug: "latest/development/partition-routing" },
            { label: "Idle queue internals", slug: "latest/development/idle-queue-internals" },
          ],
        },
      ],
    }),
  ],
  vite: {
    plugins: [tailwindcss()],
  },
});
