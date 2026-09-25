import { scenes, model, assignments, delivery, clamp } from "./scenarios.mjs";
const esc = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
const color = (kind) => `var(--story-${kind || "data"})`;
const sprite = (frame) => `/brand/sprites/ring-${frame}-128.png`;
const ids = ["a", "b", "c", "d"];
const bounds = (layout) =>
  layout === "cluster"
    ? ids
        .slice(0, 3)
        .map((id, i) => ({ id, x: 70 + i * 320, y: 110, w: 200, h: 205 }))
    : ids.map((id, i) => ({ id, x: 20 + i * 240, y: 55, w: 200, h: 190 }));
function route(from, to, nodes, index = 0) {
  const a = nodes.find((n) => n.id === from),
    b = nodes.find((n) => n.id === to);
  if (from === "client" || to === "client") {
    const node = from === "client" ? b : a;
    const p = [
      { x: 490, y: 64 },
      { x: 490, y: 90 },
      { x: node.x + 100, y: 80 },
      { x: node.x + 100, y: node.y },
    ];
    return from === "client" ? p : p.reverse();
  }
  const depth = Math.max(a.y + a.h, b.y + b.h) + 65 + (index % 3) * 21;
  return [
    { x: a.x + a.w / 2, y: a.y + a.h },
    { x: a.x + a.w / 2, y: depth },
    { x: b.x + b.w / 2, y: depth },
    { x: b.x + b.w / 2, y: b.y + b.h },
  ];
}
function at(p, t) {
  const u = 1 - t;
  return {
    x:
      u * u * u * p[0].x +
      3 * u * u * t * p[1].x +
      3 * u * t * t * p[2].x +
      t * t * t * p[3].x,
    y:
      u * u * u * p[0].y +
      3 * u * u * t * p[1].y +
      3 * u * t * t * p[2].y +
      t * t * t * p[3].y,
  };
}
const path = (p) =>
  `M${p[0].x},${p[0].y} C${p[1].x},${p[1].y} ${p[2].x},${p[2].y} ${p[3].x},${p[3].y}`;
function machine(n, label, role, dead, inactive, client = false) {
  const x = n.x,
    y = n.y;
  const screen = client
    ? `<rect x="${x + 51}" y="${y + 59}" width="98" height="64" rx="5" class="machine-screen"/><path d="M${x + 39} ${y + 132}h122l-12 8H${x + 51}Z" fill="var(--story-line)"/><text x="${x + 100}" y="${y + 99}" text-anchor="middle" font-family="monospace" font-size="19">${n.id === "a" ? "&gt;_" : "{ }"}</text>`
    : `<rect x="${x + 30}" y="${y + 46}" width="140" height="111" rx="8" class="machine-screen"/><g data-sway="${n.id}"><image data-sprite="${n.id}" class="sprite" x="${x + 52}" y="${y + 52}" width="96" height="96" href="${sprite(dead ? "dead" : "open-a")}"/></g>`;
  return `<g class="${dead ? "machine-dead" : ""} ${inactive ? "machine-inactive" : ""}"><rect x="${x}" y="${y}" width="${n.w}" height="${n.h}" rx="13" class="machine"/>
    <text x="${x + 17}" y="${y + 27}" class="machine-label">${esc(label)}</text><circle class="machine-led" cx="${x + 178}" cy="${y + 22}" r="4"/>
    ${screen}<path d="M${x + 14} ${y + n.h - 37}h172" class="machine-rule"/><text x="${x + 100}" y="${y + n.h - 15}" class="machine-role" text-anchor="middle">${esc(role)}</text>
    <path d="M${x + 15} ${y + 60}h6m-6 7h6m-6 7h6" class="machine-rule"/></g>`;
}
function wires(events, nodes, prefix) {
  return events
    .map((e, i) => {
      const p = route(e.from, e.to, nodes, i),
        d = path(p),
        t = e.progress ?? 0,
        point = at(p, t),
        label = at(p, 0.5),
        c = color(e.kind);
      return `<path d="${d}" class="wire wire-guide" stroke="${c}"/>
      <path d="${d}" class="wire wire-drawn" stroke="${c}" pathLength="1" stroke-dasharray="1" stroke-dashoffset="${1 - t}" ${t === 1 ? `marker-end="url(#${prefix}-${e.kind || "data"})"` : ""}/>
      ${t > 0 && t < 1 ? `<rect x="${point.x - 6}" y="${point.y - 5}" width="12" height="10" rx="2" fill="${c}"/>` : ""}
      ${e.active !== false && t > 0 ? `<text x="${label.x}" y="${label.y - 9}" text-anchor="middle" class="wire-label" style="fill:${c}">${esc(e.label)}</text>` : ""}`;
    })
    .join("");
}
function timeline(segments, t) {
  const left = 270,
    width = 660,
    top = 378;
  return (
    `<text x="24" y="351" class="diagram-note">PERSISTENCE &amp; DELIVERY / ILLUSTRATIVE TIME →</text>` +
    delivery.lanes
      .map(
        (name, i) =>
          `<text x="24" y="${top + i * 42 + 17}" font-size="11" style="fill:var(--story-muted)">${name}</text><rect x="${left}" y="${top + i * 42}" width="${width}" height="24" rx="4" fill="var(--story-panel)"/>`,
      )
      .join("") +
    segments
      .map(
        (
          s,
        ) => `<rect x="${left + (width * s.start) / 12}" y="${top + s.lane * 42}" width="${(width * (s.end - s.start)) / 12}" height="24" rx="4" fill="${color(s.kind)}" opacity=".09"/>
      <rect x="${left + (width * s.start) / 12}" y="${top + s.lane * 42}" width="${((width * (s.end - s.start)) / 12) * s.progress}" height="24" rx="4" fill="${color(s.kind)}" opacity=".3"/>
      <text x="${left + (width * s.start) / 12 + 6}" y="${top + s.lane * 42 + 16}" font-size="10" opacity="${s.progress > 0 ? 1 : 0.4}">${esc(s.label)}${s.progress === 1 ? " ✓" : ""}</text>`,
      )
      .join("") +
    `<path d="M${left + (width * t) / 12} ${top - 8}v${42 * 4}" stroke="var(--story-text)" stroke-opacity=".35" stroke-dasharray="3 4"/><text x="930" y="582" text-anchor="end" class="diagram-note">Window length is choreography, not measured latency</text>`
  );
}
function checkpointHistory(h, progress) {
  const base = h.accepted
    ? "Accepted base · events before 120"
    : h.status === "conflict"
      ? "Proposal blocked · conflicting evidence"
      : h.status === "waiting"
        ? "Proposal waiting · missing replica"
        : "Proposed base · awaiting agreement";
  const baseColor = color(h.status === "conflict" ? "failure" : h.status === "waiting" ? "delivery" : "control");
  return `<text x="70" y="76" class="diagram-note">A snapshot becomes a recovery base only after verified agreement</text>
    <text x="70" y="444" class="diagram-note">EVENT HISTORY / EXCLUSIVE CUT 120</text>
    <rect x="70" y="458" width="520" height="34" rx="5" fill="${baseColor}" opacity="${h.accepted ? ".3" : ".1"}"/>
    <text x="84" y="480" font-size="13">${base}</text>
    <path d="M600 449v54" stroke="var(--story-control)" stroke-dasharray="3 3"/>
    <rect x="610" y="458" width="${h.suffix ? 300 * progress : 0}" height="34" rx="5" fill="var(--story-data)" opacity=".25"/>
    <text x="624" y="480" font-size="12">${h.suffix ? "Later suffix · events ≥ 120" : "New events will follow here"}</text>
    <text x="70" y="532" class="diagram-note">LIVE PAYLOADS / INDEPENDENT OF THE EVENT CUT</text>
    ${[0, 1, 2, 3, 4, 5, 6, 7].map((i) => `<rect x="${70 + i * 106}" y="548" width="90" height="30" rx="4" fill="var(--story-${h.verified ? "ack" : "delivery"})" opacity=".22"/><text x="${115 + i * 106}" y="568" text-anchor="middle" font-size="11">${h.verified ? "verified ✓" : "live body"}</text>`).join("")}
    <text x="70" y="609" class="diagram-note">${h.verified ? "Required live bytes checked · activation barriers still remain" : "Unsettled messages retain their payloads even when their enqueue predates the cut"}</text>`;
}
export function registerStories() {
  if (customElements.get("broker-story")) return;
  customElements.define(
    "broker-story",
    class extends HTMLElement {
      connectedCallback() {
        if (this.abort) return;
        this.abort = new AbortController();
        const signal = this.abort.signal;
        this.scene = this.dataset.scene;
        this.config = scenes[this.scene];
        this.variant = this.config.variants[0].id;
        this.svg = this.querySelector("[data-stage]");
        this.position = this.querySelector("[data-position]");
        this.markerPrefix = `story-${crypto.randomUUID()}`;
        this.time = 0;
        this.clock = 0;
        this.speed = 1;
        this.last = 0;
        this.rendered = 0;
        this.key = null;
        this.visible = false;
        this.motion = matchMedia("(prefers-reduced-motion: reduce)");
        this.playing = !this.motion.matches;
        this.querySelector("[data-motion-note]").textContent = this.motion
          .matches
          ? " Reduced motion: use Next or the position slider."
          : "";
        this.motion.addEventListener(
          "change",
          () => {
            if (this.motion.matches) this.playing = false;
            this.syncControls();
          },
          { signal },
        );
        this.addEventListener(
          "click",
          (e) => {
            const button = e.target.closest("button");
            if (!button) return;
            if (button.hasAttribute("data-variant")) {
              this.variant = button.dataset.variant;
              this.seek(0);
              this.querySelectorAll("[data-variant]").forEach((b) =>
                b.setAttribute("aria-pressed", String(b === button)),
              );
            }
            if (button.hasAttribute("data-play")) {
              if (this.time >= this.current.duration) this.time = 0;
              this.playing = !this.playing;
              this.last = 0;
              this.syncControls();
              this.wake();
            }
            if (button.hasAttribute("data-replay")) {
              this.seek(0);
              this.playing = !this.motion.matches;
              this.syncControls();
              this.wake();
            }
            if (button.hasAttribute("data-next"))
              this.seek(
                this.current.steps.find((t) => t > this.time + 0.01) ??
                  this.current.duration,
              );
            if (button.hasAttribute("data-prev"))
              this.seek(
                [...this.current.steps]
                  .reverse()
                  .find((t) => t < this.time - 0.01) ?? 0,
              );
            if (button.hasAttribute("data-export")) this.exportFrame();
          },
          { signal },
        );
        this.position.addEventListener(
          "input",
          () => this.seek(Number(this.position.value)),
          { signal },
        );
        this.querySelector("[data-speed]").addEventListener(
          "change",
          (e) => {
            this.speed = Number(e.target.value);
          },
          { signal },
        );
        document.addEventListener(
          "visibilitychange",
          () => {
            this.last = 0;
            if (!document.hidden) this.wake();
          },
          { signal },
        );
        this.observer = new IntersectionObserver(
          (entries) => {
            this.visible = entries[0].isIntersecting;
            this.last = 0;
            if (this.visible) this.wake();
          },
          { threshold: 0.35 },
        );
        this.observer.observe(this.querySelector(".story-canvas"));
        this.render();
        this.syncControls();
        this.dataset.ready = "true";
      }
      disconnectedCallback() {
        this.abort?.abort();
        this.abort = null;
        this.observer?.disconnect();
        cancelAnimationFrame(this.raf);
        this.raf = 0;
      }
      seek(t) {
        this.time = Math.max(0, Math.min(this.current?.duration ?? 100, t));
        this.playing = false;
        this.last = 0;
        this.render();
        this.syncControls();
        this.querySelector("[data-announcement]").textContent =
          `Step ${this.current.index + 1} of ${this.current.count}: ${this.current.title}`;
      }
      syncControls() {
        const b = this.querySelector("[data-play]");
        b.disabled = this.motion.matches;
        b.textContent =
          this.time >= this.current.duration
            ? "Play again"
            : this.playing
              ? "Pause"
              : "Play";
        b.setAttribute(
          "aria-label",
          this.playing ? "Pause animation" : "Play animation",
        );
        this.position.max = String(this.current.duration);
        this.querySelector("[data-prev]").disabled = this.time === 0;
        this.querySelector("[data-next]").disabled =
          this.time >= this.current.duration;
      }
      wake() {
        if (
          !this.raf &&
          this.visible &&
          !document.hidden &&
          this.playing &&
          !this.motion.matches
        )
          this.raf = requestAnimationFrame((t) => this.tick(t));
      }
      tick(now) {
        this.raf = 0;
        if (
          !this.visible ||
          document.hidden ||
          !this.playing ||
          this.motion.matches
        )
          return;
        if (this.last) {
          const delta = Math.min(100, now - this.last);
          this.time +=
            (delta / 1000) *
            this.speed *
            (this.scene === "delivery" ? 0.45 : 0.7);
          this.clock += delta;
        }
        this.last = now;
        this.time = Math.min(this.time, this.current.duration);
        if (now - this.rendered >= 40 || this.time >= this.current.duration) {
          this.render();
          this.rendered = now;
        }
        if (this.time >= this.current.duration) {
          this.playing = false;
          this.syncControls();
        } else this.wake();
      }
      render() {
        const m = (this.current = model(this.scene, this.variant, this.time)),
          nodes = bounds(this.config.layout);
        const key = `${this.variant}:${m.index}`;
        if (this.key !== key) {
          this.key = key;
          this.querySelector("[data-headline]").textContent = m.title.replace(
            /^\d+ \/ /,
            "",
          );
          this.querySelector("[data-narrative]").textContent = m.text;
          this.querySelector("[data-chapter]").textContent =
            `${String(m.index + 1).padStart(2, "0")} / ${String(m.count).padStart(2, "0")}`;
          this.querySelector("[data-badge]").textContent = m.badge;
          this.querySelector("[data-contract]").textContent =
            this.scene === "delivery"
              ? delivery.variants.find((v) => v.id === this.variant).text
              : this.scene === "failover"
                ? "A owns the queue. B and C hold replicas. Follow the handoff—and the return."
                : this.config.note;
          const caption = this.querySelector(".story-caption");
          caption.removeAttribute("data-enter");
          requestAnimationFrame(() => {
            if (this.isConnected && this.playing)
              caption.setAttribute("data-enter", "");
          });
          const markers = ["data", "control", "delivery", "ack", "failure"]
            .map(
              (k) =>
                `<marker id="${this.markerPrefix}-${k}" markerWidth="7" markerHeight="7" refX="6" refY="3" orient="auto"><path d="M0 0L6 3L0 6" fill="none" stroke="${color(k)}" stroke-width="1.2"/></marker>`,
            )
            .join("");
          this.svg.innerHTML = `<defs>${markers}</defs><g data-wires></g><g data-machines>${nodes.map((n, i) => machine(n, this.config.nodes[i], m.roles[i], m.dead?.includes(n.id), m.inactive?.includes(n.id), this.scene === "delivery" && (i === 0 || i === 3))).join("")}</g><g data-extras></g><g data-timeline></g>`;
          const extra = this.svg.querySelector("[data-extras]");
          if (this.scene === "failover")
            extra.innerHTML = `<rect x="390" y="20" width="200" height="44" rx="22" class="machine"/><text x="490" y="47" text-anchor="middle" font-size="14">Client connection</text>${m.checkpoint ? '<rect x="402" y="76" width="176" height="22" rx="5" fill="var(--story-control)" opacity=".13"/><text x="490" y="91" text-anchor="middle" class="diagram-note">verified checkpoint + suffix</text>' : ""}<text x="490" y="438" text-anchor="middle" class="diagram-note">${m.blocked ? "FENCED / no serving without the required evidence" : "Queue owner ≠ metadata leader · C hosts the controller in this story"}</text>`;
          if (this.scene === "placement")
            extra.innerHTML =
              assignments
                .map((q, qi) =>
                  nodes
                    .map((n) => {
                      const owner = q.owner === n.id,
                        replica = q.followers.includes(n.id);
                      if (!owner && !replica) return "";
                      return `<rect x="${n.x}" y="${330 + qi * 32}" width="200" height="26" rx="4" fill="${owner ? color(q.color) : "none"}" fill-opacity=".2" stroke="${color(q.color)}" opacity="${m.focus === qi ? 1 : 0.25}"/><text x="${n.x + 100}" y="${347 + qi * 32}" text-anchor="middle" font-size="11" opacity="${m.focus === qi ? 1 : 0.4}">${q.name} · ${owner ? "OWNER" : "copy"}</text>`;
                    })
                    .join(""),
                )
                .join("") +
              '<text x="490" y="452" text-anchor="middle" class="diagram-note">Filled = owner · outline = follower copy · each partition has its own assignment</text>';
          this.syncControls();
        }
        const events =
          m.events ||
          m.flows.map((f, i) => ({
            ...f,
            progress: this.playing
              ? clamp((m.stepProgress - i * 0.13) / 0.7)
              : 1,
            active: true,
          }));
        this.svg.querySelector("[data-wires]").innerHTML = wires(
          events,
          nodes,
          this.markerPrefix,
        );
        if (this.scene === "checkpoint") {
          const h = m.history;
          const progress = this.playing && h.growing ? m.stepProgress : 1;
          this.svg.querySelector("[data-timeline]").innerHTML =
            checkpointHistory(h, progress);
        }
        if (this.scene === "delivery")
          this.svg.querySelector("[data-timeline]").innerHTML = timeline(
            m.segments,
            this.time,
          );
        nodes.forEach((n, i) => {
          const image = this.svg.querySelector(`[data-sprite="${n.id}"]`);
          if (!image) return;
          const dead = m.dead?.includes(n.id),
            still = dead || m.inactive?.includes(n.id) || this.motion.matches;
          const tick = Math.floor((this.clock + i * 610) / 180),
            blink = tick % 37;
          const frame = dead
            ? "dead"
            : still
              ? "open-a"
              : blink === 33 || blink === 35
                ? "half"
                : blink === 34
                  ? "closed"
                  : ["open-a", "open-b", "open"][Math.floor(tick / 3) % 3];
          if (image.getAttribute("href") !== sprite(frame))
            image.setAttribute("href", sprite(frame));
          const pose = Math.floor(tick / 3) % 3,
            y = still ? 0 : [0, -2, 1][pose],
            sx = still ? 1 : [1, 0.985, 1.01][pose],
            sy = still ? 1 : [1, 1.02, 0.99][pose];
          this.svg
            .querySelector(`[data-sway="${n.id}"]`)
            .setAttribute(
              "transform",
              `translate(${n.x + 100} ${n.y + 104 + y}) scale(${sx} ${sy}) translate(${-n.x - 100} ${-n.y - 104})`,
            );
        });
        this.position.value = String(this.time);
        this.position.setAttribute(
          "aria-valuetext",
          `Step ${m.index + 1} of ${m.count}: ${m.title}`,
        );
        this.svg.setAttribute("aria-label", `${m.title}. ${m.text}`);
      }
      async exportFrame() {
        this.playing = false;
        this.syncControls();
        const status = this.querySelector("[data-export-status]");
        try {
          const original = this.svg,
            copy = original.cloneNode(true),
            all = [original, ...original.querySelectorAll("*")],
            clones = [copy, ...copy.querySelectorAll("*")];
          const properties = [
            "fill",
            "stroke",
            "stroke-width",
            "stroke-opacity",
            "fill-opacity",
            "font-family",
            "font-size",
            "font-weight",
            "opacity",
            "paint-order",
            "image-rendering",
          ];
          all.forEach((el, i) => {
            const css = getComputedStyle(el);
            for (const name of properties)
              clones[i].style.setProperty(name, css.getPropertyValue(name));
          });
          for (const image of copy.querySelectorAll("image")) {
            const response = await fetch(image.getAttribute("href"));
            if (!response.ok) throw Error("Sprite unavailable");
            const blob = await response.blob();
            const data = await new Promise((resolve, reject) => {
              const reader = new FileReader();
              reader.onload = () => resolve(reader.result);
              reader.onerror = reject;
              reader.readAsDataURL(blob);
            });
            image.setAttribute("href", data);
          }
          const h = original.viewBox.baseVal.height,
            ns = "http://www.w3.org/2000/svg";
          const out = document.createElementNS(ns, "svg");
          out.setAttribute("xmlns", ns);
          out.setAttribute("viewBox", `0 0 980 ${h + 160}`);
          out.setAttribute("width", "980");
          out.setAttribute("height", String(h + 160));
          const bg = getComputedStyle(this).getPropertyValue("--story-bg"),
            fg = getComputedStyle(this.querySelector("h3")).color;
          const rect = document.createElementNS(ns, "rect");
          rect.setAttribute("width", "100%");
          rect.setAttribute("height", "100%");
          rect.setAttribute("fill", bg);
          out.append(rect);
          const title = document.createElementNS(ns, "text");
          title.setAttribute("x", "24");
          title.setAttribute("y", "35");
          title.setAttribute("fill", fg);
          title.setAttribute("font-family", "sans-serif");
          title.setAttribute("font-size", "21");
          title.textContent = this.current.title;
          out.append(title);
          const words = (
            this.current.text +
            " " +
            this.current.badge +
            " · Illustrative, not measured time."
          ).split(" ");
          let line = "",
            row = 0;
          for (const word of [...words, ""]) {
            if ((line + " " + word).length > 112 || word === "") {
              const text = document.createElementNS(ns, "text");
              text.setAttribute("x", "24");
              text.setAttribute("y", String(62 + row++ * 19));
              text.setAttribute("font-size", "13");
              text.setAttribute("font-family", "sans-serif");
              text.setAttribute("fill", fg);
              text.textContent = line;
              out.append(text);
              line = word;
            } else line += (line ? " " : "") + word;
          }
          copy.setAttribute("y", "145");
          copy.setAttribute("width", "980");
          copy.setAttribute("height", String(h));
          out.append(copy);
          const blob = new Blob([new XMLSerializer().serializeToString(out)], {
              type: "image/svg+xml",
            }),
            url = URL.createObjectURL(blob),
            a = document.createElement("a");
          a.href = url;
          a.download = `fibril-${this.scene}-${this.variant}-step-${this.current.index + 1}.svg`;
          a.click();
          setTimeout(() => URL.revokeObjectURL(url), 1000);
          status.textContent = " Frame saved as a self-contained SVG.";
        } catch {
          status.textContent =
            " Could not export this frame. Please retry after the artwork loads.";
        }
      }
    },
  );
}
