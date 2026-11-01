(function () {
  "use strict";
  const started = performance.now();
  const root = document.querySelector("#sections");
  const status = document.querySelector("#render-status");
  const text = (tag, value, className) => {
    const node = document.createElement(tag);
    node.textContent = value === null || value === undefined ? "Não informado" : String(value);
    if (className) node.className = className;
    return node;
  };
  const number = value => typeof value === "number" && Number.isFinite(value)
    ? new Intl.NumberFormat("pt-BR", {maximumFractionDigits: 4}).format(value) : "—";
  const colors = () => {
    const style = getComputedStyle(document.documentElement);
    return Object.fromEntries(["ink", "muted", "line", "accent", "amber", "invalid", "surface"]
      .map(name => [name, style.getPropertyValue("--" + name).trim()]));
  };
  const charts = new Map();
  const rendered = new Map();
  // Tema sem recarregar nem reconsultar: só tokens CSS e redesenho dos gráficos já montados.
  const THEMES = ["auto", "light", "dark"], THEME_LABEL = {auto: "automático", light: "claro", dark: "escuro"};
  function applyTheme(theme) {
    if (theme === "auto") delete document.documentElement.dataset.theme;
    else document.documentElement.dataset.theme = theme;
    const button = document.querySelector("#theme-toggle");
    if (button) {
      button.textContent = "Tema: " + THEME_LABEL[theme];
      button.setAttribute("aria-label", "Tema atual " + THEME_LABEL[theme] + "; alternar tema");
      button.dataset.theme = theme;
    }
    const palette = colors();
    // Cópia: reinserir a mesma chave durante forEach de um Map a revisita sem fim.
    [...charts].forEach(([id, chart]) => {
      if (typeof chart.mt5xTheme === "function") { chart.mt5xTheme(palette); return; }
      const entry = rendered.get(id);
      chart.dispose();
      charts.delete(id);
      if (entry && entry.figure.isConnected) {
        const again = entry.renderer(entry.figure, entry.section, palette);
        if (again) charts.set(id, again);
      }
    });
  }
  function initTheme() {
    let stored = "auto";
    try { stored = localStorage.getItem("mt5x-dashboard-theme") || "auto"; } catch (error) { stored = "auto"; }
    if (!THEMES.includes(stored)) stored = "auto";
    const button = document.querySelector("#theme-toggle");
    if (!button) return;
    button.hidden = false;
    applyTheme(stored);
    button.addEventListener("click", () => {
      const next = THEMES[(THEMES.indexOf(button.dataset.theme || "auto") + 1) % THEMES.length];
      try { localStorage.setItem("mt5x-dashboard-theme", next); } catch (error) { /* só conveniência local */ }
      applyTheme(next);
    });
    matchMedia("(prefers-color-scheme: dark)").addEventListener("change", () => {
      if ((button.dataset.theme || "auto") === "auto") applyTheme("auto");
    });
  }
  const HUMAN = new Intl.DateTimeFormat("pt-BR", {timeZone: "UTC", day: "2-digit", month: "short", year: "numeric",
    hour: "2-digit", minute: "2-digit"});
  const human = iso => iso ? HUMAN.format(new Date(iso)) + " UTC" : "não informado";
  const age = minutes => minutes < 60 ? minutes + " min" : minutes < 2880 ? Math.round(minutes / 60) + " h"
    : Math.round(minutes / 1440) + " dias";
  function alertNode(alert) {
    // Vermelho reservado à invalidação retroativa conhecida no corte.
    const node = text("div", "", "alert");
    node.setAttribute("role", "alert");
    node.append(text("strong", "Invalidação retroativa · " + alert.feature_id + " (" + alert.algorithm_version + ")"),
      text("p", "Observação de " + human(alert.observation_time) + ": revisão " + alert.previous_revision
        + " (valor " + (typeof alert.previous_value === "number" ? number(alert.previous_value) : alert.previous_value) + ", conhecida em " + human(alert.previous_available_at_utc)
        + ") foi invalidada pela revisão " + alert.new_revision + ", conhecida em " + human(alert.known_at_utc)
        + ". Evidências que dependiam dela não usam o valor anterior."));
    return node;
  }
  const details = section => {
    const node = document.createElement("details");
    node.append(text("summary", "Ver evidência · " + section.title));
    node.append(text("p", section.details.reason || "Recorte e evidência preservados nesta fotografia."));
    const rows = document.createElement("dl");
    for (const [label, value] of [["Amostra", section.n_observations], ["Unidade", section.unit],
      ["Estado da origem", section.source_status], ["Namespace", section.status_namespace],
      ["Corte UTC", section.as_of_utc], ["Referência", section.visual.baseline ?? section.evidence.baseline]]) {
      rows.append(text("dt", label), text("dd", value));
    }
    node.append(rows);
    const p = section.priority || {};
    if (p.position) {
      const f = p.factors || {}, show = x => x && x.value !== null && x.value !== undefined ? number(x.value) : "não apurado";
      node.append(text("p", "Por que nesta posição: " + p.position + "ª da narrativa · " + (p.group_label || p.group)
        + (p.score_state === "AVAILABLE" ? " · peso " + number(p.score) + " no grupo (I " + show(f.I) + " × E "
          + show(f.E) + " × N " + show(f.N) + (f.N && f.N.label ? ", " + f.N.label : "") + ")"
          : " · " + (p.reason || "sem score")) + " · política " + p.policy + ". Ordem visual, não avaliação.", "priority-note"));
    }
    const audit = document.createElement("details");
    audit.append(text("summary", "Contrato, fontes e dados desta faixa"), text("pre", JSON.stringify(section, null, 2)));
    node.append(audit);
    return node;
  };
  function sectionNode(section) {
    const fragment = document.querySelector("#section-template").content.cloneNode(true);
    const node = fragment.querySelector("section");
    node.dataset.sectionId = section.id;
    node.dataset.state = section.display_state;
    node.querySelector(".section-kind").textContent = ({surprise: "SURPRESA", regime: "REGIME", volume: "VOLUME",
      hypothesis: "HIPÓTESE", validation: "VALIDAÇÃO", robot: "ROBÔ"})[section.kind] || section.kind;
    node.querySelector("h2").textContent = section.title;
    node.querySelector(".section-state").textContent = ({AVAILABLE: "", UNAVAILABLE: "Indisponível",
      INSUFFICIENT: "Amostra insuficiente", STALE: "Validade encerrada", INVALIDATED: "Invalidada",
      NOT_APPLICABLE: "Não se aplica"})[section.display_state] ?? section.display_state;
    if (section.display_state === "AVAILABLE" && section.details.status_label)
      node.querySelector(".section-state").textContent = section.details.status_label;
    const body = node.querySelector(".section-body");
    const stale = section.display_state === "STALE" && section.temporal && section.temporal.stale;
    if (stale) body.append(text("p", "Validade encerrada em " + human(stale.valid_until_utc) + " · "
      + age(stale.expired_minutes_before_cut) + " antes do corte · observado em " + human(stale.observation_time)
      + ". Não é o estado atual.", "stale-note"));
    body.append(text("p", section.n_observations === null ? "n não informado" : "n = " + section.n_observations, "sample"));
    if (section.visual.value !== null && section.visual.value !== undefined) {
      const metric = text("button", "", "metric");
      metric.type = "button";
      metric.setAttribute("aria-label", "Aprofundar " + section.title);
      metric.append(text("span", ({UP: "↑", DOWN: "↓", FLAT: "→"})[section.visual.direction] || "", "direction"),
        text("strong", typeof section.visual.value === "number" ? (Number.isInteger(section.visual.decimals)
          ? new Intl.NumberFormat("pt-BR", {minimumFractionDigits: section.visual.decimals,
            maximumFractionDigits: section.visual.decimals}).format(section.visual.value) : number(section.visual.value))
          : section.visual.value),
        text("span", section.unit || "", "unit"));
      body.append(metric);
      if (section.visual.baseline !== null && section.visual.baseline !== undefined)
        body.append(text("p", "Referência " + number(section.visual.baseline) + " " + (section.unit || ""), "reference"));
      const detail = details(section);
      metric.addEventListener("click", () => { detail.open = !detail.open; });
      body.append(detail);
      if ((section.visual.series || []).length || (section.visual.cells || []).length || (section.visual.rings || []).length || (section.visual.folds || []).length) {
        const figure = text("div", "", "chart chart--" + section.kind);
        figure.tabIndex = 0;
        figure.setAttribute("role", "img");
        figure.setAttribute("aria-label", section.title + ". " + (section.visual.series_label || "Série conhecida no corte")
          + "; valores em Ver evidência.");
        figure.addEventListener("click", () => { detail.open = true; });
        figure.addEventListener("keydown", event => { if (["Enter", " "].includes(event.key)) {event.preventDefault(); detail.open = !detail.open;} });
        body.insertBefore(figure, detail);
        requestAnimationFrame(() => {
          const renderer = window.MT5XCharts[section.kind];
          if (renderer) {
            const chart = renderer(figure, section, colors());
            if (chart) { charts.set(section.id, chart); rendered.set(section.id, {figure, section, renderer}); }
          }
        });
      }
    } else {
      body.append(text("p", section.details.reason || "Sem evidência disponível neste recorte.", "absence"), details(section));
    }
    if (section.default_visible) {
      // Descarte só de apresentação: não reprova, não retira nem altera a origem.
      const dismiss = text("button", "Ocultar faixa", "dismiss");
      dismiss.type = "button";
      dismiss.setAttribute("aria-label", "Ocultar " + section.title + " desta leitura; pode ser restaurada abaixo");
      dismiss.addEventListener("click", () => dismissSection(section, node));
      node.querySelector(".section-heading").append(dismiss);
    }
    return node;
  }
  function release(id) {
    const chart = charts.get(id);
    if (chart) { chart.dispose(); charts.delete(id); }
    rendered.delete(id);
  }
  const BALANCE = '<svg viewBox="0 0 24 24" width="20" height="20" aria-hidden="true" fill="none" stroke="currentColor" '
    + 'stroke-width="1.6"><path d="M12 3v18M7 21h10M4 9l8-2 8 -3"/><path d="M1.5 14 4 9l2.5 5a2.5 2.5 0 0 1-5 0Z"/>'
    + '<path d="M17.5 9 20 4l2.5 5a2.5 2.5 0 0 1-5 0Z"/></svg>';
  const sampleText = list => list.some(s => s.n_observations === null || s.n_observations === undefined)
    ? "n não informado" + (list.some(s => typeof s.n_observations === "number")
      ? " em parte · n conhecido " + list.reduce((t, s) => t + (s.n_observations || 0), 0) : "")
    : "n " + list.reduce((t, s) => t + s.n_observations, 0);
  function hiddenGroup(label, list, removable) {
    const row = text("div", "", "hidden-row");
    const toggle = text("button", "", "hidden-toggle");
    toggle.type = "button";
    toggle.setAttribute("aria-expanded", "false");
    toggle.append(text("span", label, "hidden-reason"), text("span", list.length + (list.length === 1 ? " faixa" : " faixas")
      + " · " + sampleText(list), "hidden-count"));
    const holder = text("div", "", "hidden-sections");
    holder.hidden = true;
    toggle.addEventListener("click", () => {
      const open = holder.hidden;
      holder.hidden = !open;
      toggle.setAttribute("aria-expanded", String(open));
      if (open) holder.replaceChildren(...list.map(section => {
        const node = sectionNode({...section, default_visible: false});
        node.classList.add("is-hidden-view");
        node.querySelector(".section-state").textContent = "Oculta · " + (section.hidden_reason_short || label);
        if (removable) {
          const restore = text("button", "Restaurar na narrativa", "dismiss");
          restore.type = "button";
          restore.addEventListener("click", () => restoreSection(section));
          node.querySelector(".section-heading").append(restore);
        }
        return node;
      }));
      else { list.forEach(section => release(section.id)); holder.replaceChildren(); }
    });
    row.append(toggle, holder);
    return row;
  }
  let snapshotData = null;
  const dismissed = new Set();
  function hiddenPanel() {
    const panel = text("section", "", "hidden-panel");
    panel.setAttribute("aria-label", "Evidências ocultas");
    const hidden = snapshotData.sections.filter(s => !s.default_visible && !s.details.audit_only);
    const own = snapshotData.sections.filter(s => s.default_visible && dismissed.has(s.id));
    const heading = text("h2", "");
    heading.innerHTML = BALANCE;
    heading.append(text("span", " Evidências ocultas · " + (hidden.length + own.length)));
    panel.append(heading, text("p", "Não aparecem por padrão. Expandir mostra a evidência com seu estado e motivo, "
      + "sem promovê-la; ocultar uma faixa não altera a origem.", "absence"));
    const groups = new Map();
    hidden.forEach(s => {
      const key = s.hidden_reason_code || "SEM_MOTIVO";
      if (!groups.has(key)) groups.set(key, {label: s.hidden_reason_short || s.details.reason || key, list: []});
      groups.get(key).list.push(s);
    });
    groups.forEach(group => panel.append(hiddenGroup(group.label, group.list, false)));
    if (own.length) panel.append(hiddenGroup("Ocultadas por você nesta leitura", own, true));
    if (!hidden.length && !own.length) panel.append(text("p", "Nenhuma evidência oculta nesta fotografia.", "absence"));
    return panel;
  }
  function narrative() {
    snapshotData.sections.filter(s => s.default_visible).forEach(s => release(s.id));
    root.replaceChildren(...snapshotData.alerts.filter(alert => alert.kind === "RETROACTIVE_INVALIDATION").map(alertNode),
      ...snapshotData.sections.filter(s => s.default_visible && !dismissed.has(s.id)).map(sectionNode), hiddenPanel());
  }
  function dismissSection(section) { dismissed.add(section.id); release(section.id); narrative(); }
  function restoreSection(section) { dismissed.delete(section.id); release(section.id); narrative(); }
  async function main() {
    try {
      const encoded = JSON.parse(document.querySelector("#dashboard-data").textContent);
      const decoded = await window.MT5XTransport.decode(encoded);
      const data = decoded.data, digest = data.snapshot_id;
      snapshotData = data;
      initTheme();
      narrative();
      document.documentElement.dataset.snapshotId = digest;
      const facts = document.querySelector("#facts-audit");
      facts.append(text("pre", JSON.stringify({contracts: data.contracts, facts: data.facts,
        capabilities: data.capabilities, provenance: data.provenance}, null, 2)));
      const download = document.querySelector("#download-json");
      download.addEventListener("click", () => {
        const url = URL.createObjectURL(new Blob([decoded.json], {type: "application/json"}));
        const link = document.createElement("a"); link.href = url; link.download = "mt5x-" + digest.slice(0, 12) + ".json";
        link.click(); URL.revokeObjectURL(url);
      });
      requestAnimationFrame(() => requestAnimationFrame(() => {
        status.textContent = "Fotografia conferida · " + Math.round(performance.now() - started) + " ms de apresentação";
        document.documentElement.dataset.ready = "true";
      }));
    } catch (error) {
      root.replaceChildren(text("p", "Não foi possível ler a fotografia: " + error.message, "absence"));
      status.textContent = "Fotografia recusada";
      document.documentElement.dataset.ready = "error";
    }
  }
  window.addEventListener("resize", () => charts.forEach(chart => chart.resize()));
  document.addEventListener("keydown", event => {
    if (event.target.tagName === "SUMMARY" && ["Enter", " "].includes(event.key)) {
      event.preventDefault();
      event.target.parentElement.open = !event.target.parentElement.open;
    }
  });
  window.addEventListener("pagehide", () => { charts.forEach(chart => chart.dispose()); charts.clear(); });
  main();
}());
