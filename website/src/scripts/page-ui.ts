// Small helpers shared by the landing page sections.

export const reduceMotion = matchMedia("(prefers-reduced-motion: reduce)").matches;

/** Calls onChange when the element enters or leaves the viewport, or the tab is hidden. */
export function whenVisible(el: Element, onChange: (visible: boolean) => void) {
  let inView = false;
  const report = () => onChange(inView && !document.hidden);
  if ("IntersectionObserver" in window) {
    new IntersectionObserver(([entry]) => {
      inView = entry?.isIntersecting ?? false;
      report();
    }).observe(el);
  } else {
    inView = true;
  }
  document.addEventListener("visibilitychange", report);
  report();
}

/** Wires a role="tablist" row: click and arrow key selection. */
export function tablist(row: HTMLElement, onSelect: (tab: HTMLElement) => void) {
  const tabs = [...row.querySelectorAll<HTMLElement>('[role="tab"]')];
  const select = (tab: HTMLElement, focus = false) => {
    for (const t of tabs) {
      const on = t === tab;
      t.setAttribute("aria-selected", String(on));
      t.tabIndex = on ? 0 : -1;
    }
    if (focus) tab.focus();
    onSelect(tab);
  };
  row.addEventListener("click", (e) => {
    const tab = (e.target as Element).closest<HTMLElement>('[role="tab"]');
    if (tab && tabs.includes(tab)) select(tab);
  });
  row.addEventListener("keydown", (e) => {
    const i = tabs.indexOf(document.activeElement as HTMLElement);
    if (i < 0) return;
    const j = e.key === "ArrowRight" ? i + 1 : e.key === "ArrowLeft" ? i - 1 : e.key === "Home" ? 0 : e.key === "End" ? tabs.length - 1 : null;
    if (j === null) return;
    e.preventDefault();
    select(tabs[(j + tabs.length) % tabs.length], true);
  });
}

/** Shows the panel controlled by the selected tab and hides its siblings. */
export function showPanel(tab: HTMLElement) {
  const row = tab.closest('[role="tablist"]')!;
  for (const t of row.querySelectorAll<HTMLElement>('[role="tab"]')) {
    const panel = document.getElementById(t.getAttribute("aria-controls")!);
    if (panel) panel.hidden = t !== tab;
  }
}
