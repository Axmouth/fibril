// Resolve colors from the site palette so syntax follows both themes without
// maintaining another set of light/dark color values.
export const siteCodeTheme = {
  name: "fibril",
  type: "dark" as const,
  colors: {
    "editor.foreground": "var(--text)",
    "editor.background": "var(--panel)",
  },
  tokenColors: [
    {
      scope: ["keyword", "storage"],
      settings: { foreground: "var(--accent)" },
    },
    {
      scope: ["entity.name.function", "support.function"],
      settings: { foreground: "var(--ok)" },
    },
    {
      scope: ["string", "constant.numeric", "constant.language"],
      settings: { foreground: "var(--organic)" },
    },
    {
      scope: ["comment"],
      settings: { foreground: "var(--muted)" },
    },
    {
      scope: ["punctuation", "keyword.operator"],
      settings: { foreground: "var(--muted)" },
    },
  ],
};
