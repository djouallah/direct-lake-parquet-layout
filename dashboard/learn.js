// Shared by the static learning pages (intro, parquet, transcode, writers). Published beside them by
// dashboard.yml. It builds the top tab bar, shows one section at a time (the hash names it, so links
// and the back button work), adds a pager under each section that rolls over into the next tab, and
// lets ← / → (or a presentation clicker's PageUp / PageDown) drive the whole tour like slides.
//
// THE TAB LIST IS ALSO WRITTEN OUT IN index.html, which build.mjs bundles separately: change both.
(() => {
  const I = {
    intro: '<circle cx="12" cy="12" r="9.5"/><path d="M12 11v6M12 7.5v.01"/>',
    parquet: '<rect x="3" y="4" width="18" height="16" rx="2"/><path d="M3 9.5h18M3 15h18M9 4v16M15 4v16"/>',
    transcode: '<path d="M4 8h13l-3.5-3.5M20 16H7l3.5 3.5"/>',
    exp: '<path d="M4 20V10M10 20V4M16 20v-7M22 20H2"/>',
    perf: '<path d="M3 3v18h18"/><circle cx="8" cy="15" r="1.6"/><circle cx="12" cy="9" r="1.6"/><circle cx="17" cy="12" r="1.6"/><circle cx="19" cy="5" r="1.6"/>',
    writers: '<path d="M4 20l4-1 11-11-3-3L5 16z"/><path d="M14 7l3 3"/>',
  };
  // [key, href, label]. Order is the order of the talk.
  const TABS = [
    ["intro", "intro.html", "Introduction"],
    ["parquet", "parquet.html", "Parquet"],
    ["transcode", "transcode.html", "Transcoding"],
    ["exp", "./", "Experiments"],
    ["perf", "./?page=perf", "Performance"],
    ["writers", "writers.html", "Third-party writers"],
  ];
  const GH = '<svg viewBox="0 0 16 16" fill="currentColor" aria-hidden="true"><path d="M8 0c4.42 0 8 3.58 8 8a8.013 8.013 0 0 1-5.45 7.59c-.4.08-.55-.17-.55-.38 0-.27.01-1.13.01-2.2 0-.75-.25-1.23-.54-1.48 1.78-.2 3.65-.88 3.65-3.95 0-.88-.31-1.59-.82-2.15.08-.2.36-1.02-.08-2.12 0 0-.67-.22-2.2.82-.64-.18-1.32-.27-2-.27-.68 0-1.36.09-2 .27-1.53-1.03-2.2-.82-2.2-.82-.44 1.1-.16 1.92-.08 2.12-.51.56-.82 1.28-.82 2.15 0 3.06 1.86 3.75 3.64 3.95-.23.2-.44.55-.51 1.07-.46.21-1.61.55-2.33-.66-.15-.24-.6-.83-1.23-.82-.67.01-.27.38.01.53.34.19.73.9.82 1.13.16.45.68 1.31 2.69.94 0 .67.01 1.3.01 1.49 0 .21-.15.45-.55.38A7.995 7.995 0 0 1 0 8c0-4.42 3.58-8 8-8Z"/></svg>';
  const here = document.body.dataset.tab;
  const at = TABS.findIndex(([k]) => k === here);

  const bar = document.querySelector(".topbar");
  const tabs = document.createElement("nav");
  tabs.className = "tabs"; tabs.setAttribute("aria-label", "pages");
  tabs.innerHTML = TABS.map(([k, h, l]) => `<a href="${h}"${k === here ? ' aria-current="page"' : ""}><svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">${I[k]}</svg>${l}</a>`).join("")
    + `<a class="gh" href="https://github.com/djouallah/direct-lake-parquet-layout" title="Source on GitHub" aria-label="Source on GitHub">${GH}</a>`;
  bar.prepend(tabs);
  const cur = tabs.querySelector('[aria-current="page"]');
  if (cur) tabs.scrollLeft = cur.offsetLeft - 16;

  document.documentElement.classList.add("js");
  const panes = [...document.querySelectorAll(".pane")];
  const subs = [...document.querySelectorAll(".subtabs a")];
  const prevTab = TABS[at - 1], nextTab = TABS[at + 1];
  // A learning page is entered at its LAST section when arriving backwards, so ← reads like a deck.
  const isLearn = (h) => /\.html$/.test(h);
  panes.forEach((pane, i) => {
    const prev = panes[i - 1], next = panes[i + 1];
    const nav = document.createElement("nav");
    nav.className = "pager";
    const back = prev ? `<a class="prev" href="#${prev.dataset.name}">← ${prev.getAttribute("aria-label")}</a>`
      : prevTab ? `<a class="prev cross" href="${prevTab[1]}${isLearn(prevTab[1]) ? "#last" : ""}">← ${prevTab[2]}</a>` : "";
    const fwd = next ? `<a class="next" href="#${next.dataset.name}">${next.getAttribute("aria-label")} →</a>`
      : nextTab ? `<a class="next cross" href="${nextTab[1]}">${nextTab[2]} →</a>` : "";
    nav.innerHTML = back + fwd;
    pane.append(nav);
  });
  if (panes.length) {
    const k = document.createElement("p");
    k.className = "keys";
    k.innerHTML = "<kbd>←</kbd> <kbd>→</kbd> to step through, like slides";
    panes[panes.length - 1].parentElement.insertBefore(k, document.querySelector("footer"));
  }
  let current = null;
  function show() {
    const id = location.hash.slice(1);
    const pane = id === "last" ? panes[panes.length - 1] : panes.find((p) => p.dataset.name === id) || panes[0];
    if (!pane) return;
    current = pane;
    panes.forEach((p) => p.classList.toggle("on", p === pane));
    subs.forEach((t) => {
      const on = t.dataset.pane === pane.dataset.name;
      t.setAttribute("aria-selected", String(on));
      // Keep the chosen tab visible in the bar on a phone, without scrolling the page itself.
      if (on) { const b = t.parentElement; b.scrollLeft = t.offsetLeft - (b.clientWidth - t.offsetWidth) / 2; }
    });
    window.scrollTo(0, 0);
  }
  addEventListener("hashchange", show);
  show();
  addEventListener("keydown", (e) => {
    if (e.altKey || e.ctrlKey || e.metaKey || e.shiftKey || !current) return;
    const t = e.target;
    if (t && (t.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(t.tagName))) return;
    const dir = { ArrowRight: "next", PageDown: "next", ArrowLeft: "prev", PageUp: "prev" }[e.key];
    if (!dir) return;
    const a = current.querySelector(`:scope > .pager a.${dir}`);
    if (a) { e.preventDefault(); a.click(); }
  });

  // Copy buttons on code blocks.
  document.querySelectorAll("pre").forEach((pre) => {
    const b = document.createElement("button");
    b.className = "copy"; b.type = "button"; b.textContent = "copy";
    b.addEventListener("click", async () => {
      try { await navigator.clipboard.writeText(pre.querySelector("code").innerText); b.textContent = "copied"; }
      catch { b.textContent = "select + copy"; }
      setTimeout(() => (b.textContent = "copy"), 1500);
    });
    pre.append(b);
  });
})();
