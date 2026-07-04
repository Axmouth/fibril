import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import starlightVersions from "starlight-versions";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  site: "https://fibril.sh",
  // The unversioned docs at the site root are the current version (labeled
  // "Latest" in the version picker). Released versions are archived under their
  // own slug (e.g. /0.2/) by starlight-versions. These redirects keep the older
  // /latest/* URLs working after current docs moved to the root.
  redirects: {
    "/latest": "/",
    "/latest/[...slug]": "/[...slug]",
  },
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
      plugins: [
        starlightVersions({
          versions: [{ slug: "0.4" }, { slug: "0.3" }, { slug: "0.2" }],
        }),
      ],
      sidebar: [
        {
          label: "Start Here",
          items: [
            { label: "Overview", slug: "overview" },
            { label: "Quickstart", slug: "quickstart" },
            { label: "Client usage", slug: "clients" },
            { label: "Configuration", slug: "configuration" },
            { label: "Project status", slug: "status" },
            { label: "Implemented surface", slug: "implemented-surface" },
          ],
        },
        {
          label: "Concepts",
          items: [
            { label: "Core model", slug: "concepts/core-model" },
            { label: "Plexus streams", slug: "concepts/plexus-streams" },
            { label: "Consumer groups", slug: "concepts/consumer-groups" },
            { label: "Clustering and coordination", slug: "concepts/clustering" },
            { label: "Reliability semantics", slug: "reliability/semantics" },
            { label: "Retries and delays", slug: "reliability/retries-delays" },
            { label: "Dead lettering", slug: "reliability/dead-lettering" },
            { label: "Replication", slug: "reliability/replication" },
            { label: "Backpressure", slug: "concepts/backpressure" },
            { label: "Many idle queues", slug: "concepts/many-idle-queues" },
            { label: "Benchmarks", slug: "benchmarks" },
            { label: "Roadmap", slug: "roadmap" },
          ],
        },
        {
          label: "Operations",
          items: [
            { label: "Deployment", slug: "deployment/source" },
            { label: "Setting up a cluster", slug: "deployment/cluster" },
            { label: "Admin dashboard", slug: "admin-dashboard" },
            { label: "Monitoring", slug: "deployment/monitoring" },
            { label: "Failure modes and operations", slug: "reliability/failure-modes" },
            { label: "Recovery quarantine", slug: "reliability/recovery-quarantine" },
          ],
        },
        {
          label: "Development Notes",
          items: [
            { label: "Documentation style", slug: "development/docs-writing" },
            { label: "Configuration policy", slug: "development/config-policy" },
            { label: "Configuration design", slug: "development/config-design" },
            { label: "Metadata policy", slug: "development/metadata-policy" },
            { label: "Partition routing", slug: "development/partition-routing" },
            { label: "Live routing and cutover", slug: "development/live-routing-and-cutover" },
            { label: "Coordination internals", slug: "development/coordination-internals" },
            { label: "Replication design", slug: "development/replication-design" },
            { label: "Recovery quarantine internals", slug: "development/recovery-internals" },
            { label: "Deterministic simulation", slug: "development/deterministic-simulation" },
            { label: "TLS and authentication internals", slug: "development/security-internals" },
            { label: "Versioning and releasing", slug: "development/releasing" },
            { label: "Idle queue internals", slug: "development/idle-queue-internals" },
          ],
        },
      ],
    }),
  ],
  vite: {
    plugins: [tailwindcss()],
  },
});
