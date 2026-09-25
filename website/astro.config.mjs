import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import starlightVersions from "starlight-versions";
import tailwindcss from "@tailwindcss/vite";
import { sidebar } from "./src/navigation.mjs";

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
      components: { SiteTitle: "./src/components/SiteTitle.astro" },
      customCss: ["./src/styles/starlight.css"],
      favicon: "/brand/fibril-mark-48.png",
      head: [
        { tag: "link", attrs: { rel: "icon", type: "image/png", sizes: "16x16", href: "/brand/fibril-mark-16.png" } },
        { tag: "link", attrs: { rel: "icon", type: "image/png", sizes: "32x32", href: "/brand/fibril-mark-32.png" } },
        { tag: "link", attrs: { rel: "icon", type: "image/svg+xml", sizes: "any", href: "/brand/fibril-mark.svg" } },
      ],
      social: [
        { icon: "github", label: "GitHub", href: "https://github.com/Axmouth/fibril" },
      ],
      plugins: [
        starlightVersions({
          versions: [{ slug: "0.4" }, { slug: "0.3" }, { slug: "0.2" }],
        }),
      ],
      sidebar,
    }),
  ],
  vite: {
    plugins: [tailwindcss()],
  },
});
