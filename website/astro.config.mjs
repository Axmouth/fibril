import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  site: "https://fibril.sh",
  integrations: [
    starlight({
      title: "Fibril",
      description: "A lightweight Rust message broker with durable queues and explicit delivery semantics.",
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
            { label: "Project status", slug: "latest/status" },
          ],
        },
        {
          label: "Concepts",
          items: [
            { label: "Core model", slug: "latest/concepts/core-model" },
            { label: "Reliability semantics", slug: "latest/reliability/semantics" },
            { label: "Retries and delays", slug: "latest/reliability/retries-delays" },
            { label: "Dead lettering", slug: "latest/reliability/dead-lettering" },
            { label: "Backpressure", slug: "latest/concepts/backpressure" },
            { label: "Benchmarks", slug: "latest/benchmarks" },
            { label: "Roadmap", slug: "latest/roadmap" },
            { label: "Deployment", slug: "latest/deployment/source" },
          ],
        },
      ],
    }),
  ],
  vite: {
    plugins: [tailwindcss()],
  },
});
