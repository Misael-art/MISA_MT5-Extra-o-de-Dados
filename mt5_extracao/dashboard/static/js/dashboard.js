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
    const body = node.querySelector(".section-body");
    body.append(text("p", section.n_observations === null ? "n não informado" : "n = " + section.n_observations, "sample"));
    if (section.visual.value !== null && section.visual.value !== undefined) {
      const metric = text("button", "", "metric");
      metric.type = "button";
      metric.setAttribute("aria-label", "Aprofundar " + section.title);
      metric.append(text("span", ({UP: "↑", DOWN: "↓", FLAT: "→"})[section.visual.direction] || "", "direction"),
        text("strong", typeof section.visual.value === "number" ? number(section.visual.value) : section.visual.value),
        text("span", section.unit || "", "unit"));
      body.append(metric);
      if (section.visual.baseline !== null && section.visual.baseline !== undefined)
        body.append(text("p", "Referência " + number(section.visual.baseline) + " " + (section.unit || ""), "reference"));
      const detail = details(section);
      metric.addEventListener("click", () => { detail.open = !detail.open; });
      body.append(detail);
      if ((section.visual.series || []).length || (section.visual.cells || []).length) {
        const figure = text("div", "", "chart chart--" + section.kind);
        figure.tabIndex = 0;
        figure.setAttribute("role", "img");
        figure.setAttribute("aria-label", section.title + ". Série conhecida no corte; valores em Ver evidência.");
        figure.addEventListener("click", () => { detail.open = true; });
        figure.addEventListener("keydown", event => { if (["Enter", " "].includes(event.key)) {event.preventDefault(); detail.open = !detail.open;} });
        body.insertBefore(figure, detail);
        requestAnimationFrame(() => {
          const renderer = window.MT5XCharts[section.kind];
          if (renderer) { const chart = renderer(figure, section, colors()); if (chart) charts.set(section.id, chart); }
        });
      }
    } else {
      body.append(text("p", section.details.reason || "Sem evidência disponível neste recorte.", "absence"), details(section));
    }
    return node;
  }
  async function main() {
    try {
      const encoded = JSON.parse(document.querySelector("#dashboard-data").textContent);
      const decoded = await window.MT5XTransport.decode(encoded);
      const data = decoded.data, digest = data.snapshot_id;
      root.replaceChildren(...data.sections.filter(section => section.default_visible).map(sectionNode));
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
