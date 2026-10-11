// Wraps each markdown table in a scroll container so the table itself can lay
// out at the column width and wrap its text, scrolling sideways only when even
// wrapped it cannot fit. Tables with many columns are marked wide, which lets
// their page use a wider content column (see starlight.css).
const WIDE_COLUMNS = 5;

function columnCount(table) {
  const rows = [];
  const walk = (node) => {
    if (node.type === "element" && node.tagName === "tr") rows.push(node);
    for (const child of node.children ?? []) walk(child);
  };
  walk(table);
  const first = rows[0];
  if (!first) return 0;
  return first.children.filter((c) => c.type === "element" && (c.tagName === "th" || c.tagName === "td")).length;
}

export default function rehypeWrapTables() {
  return (tree) => {
    const visit = (node) => {
      const children = node.children;
      if (!children) return;
      for (let i = 0; i < children.length; i++) {
        const child = children[i];
        if (child.type === "element" && child.tagName === "table") {
          const classes = ["table-wrap"];
          if (columnCount(child) >= WIDE_COLUMNS) classes.push("table-wrap-wide");
          children[i] = {
            type: "element",
            tagName: "div",
            properties: { className: classes },
            children: [child],
          };
        } else {
          visit(child);
        }
      }
    };
    visit(tree);
  };
}
