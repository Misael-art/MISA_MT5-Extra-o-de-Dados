(() => {
  "use strict";
  const fold = value => value.normalize("NFD").replace(/\p{Diacritic}/gu, "").toLowerCase();
  const search = document.querySelector(".library-search");
  const input = document.querySelector("#library-query");
  const results = document.querySelector("#library-results");
  const sectors = [...document.querySelectorAll(".library-sector")].map(section => ({
    section,
    entries: [...section.querySelectorAll("[data-library-entry]")].map(element => ({
      element, text: fold(element.textContent + " " + section.id)
    }))
  }));
  search.hidden = false;
  input.addEventListener("input", () => {
    const words = fold(input.value).trim().split(/\s+/).filter(Boolean);
    let total = 0;
    for (const {section, entries} of sectors) {
      let visible = 0;
      for (const {element, text} of entries) {
        element.hidden = !words.every(word => text.includes(word));
        if (!element.hidden) visible++;
      }
      section.hidden = visible === 0;
      for (const heading of section.querySelectorAll(".library-subheading")) heading.hidden = words.length > 0;
      total += visible;
    }
    results.textContent = words.length ? `${total} ${total === 1 ? "ficha encontrada" : "fichas encontradas"}` : "";
  });
  document.querySelector(".library-nav").addEventListener("click", () => {
    if (!input.value) return;
    input.value = "";
    input.dispatchEvent(new Event("input"));
  });

  const figure = document.querySelector("#exemplos");
  const select = document.querySelector("#example-select");
  const chartNode = document.querySelector("#example-chart");
  const caption = document.querySelector("#example-caption");
  const values = document.querySelector("#example-values");
  if (!window.echarts) return;
  const examples = new Map(JSON.parse(figure.dataset.examples).map(example => [example.id, example]));
  chartNode.hidden = false;
  const chart = echarts.init(chartNode, null, {renderer: "svg"});
  const format = new Intl.NumberFormat("pt-BR", {maximumFractionDigits: 4});
  const display = value => value === null ? "—" : Array.isArray(value) ? value.map(v => format.format(v)).join(" / ") : format.format(value);
  const themeQuery = matchMedia("(prefers-color-scheme: dark)");
  function render() {
    const example = examples.get(select.value);
    const style = getComputedStyle(document.documentElement);
    const palette = ["accent", "amber", "focus", "invalid"].map(name => style.getPropertyValue("--" + name).trim());
    const ink = style.getPropertyValue("--ink").trim();
    const line = style.getPropertyValue("--line").trim();
    caption.textContent = example.caption;
    chartNode.setAttribute("aria-label", example.title);
    chart.setOption({
      animation: false,
      color: palette,
      textStyle: {color: ink},
      grid: {left: 48, right: 18, top: 53, bottom: 35},
      legend: {textStyle: {color: ink}, top: 1, type: "scroll"},
      tooltip: {trigger: "axis", confine: true, valueFormatter: display},
      xAxis: {type: "category", data: example.labels, axisLabel: {color: ink, hideOverlap: true}},
      yAxis: {type: "value", name: example.unit, scale: true, min: example.min, max: example.max,
        nameTextStyle: {color: ink, align: "left"}, axisLabel: {color: ink}, splitLine: {lineStyle: {color: line}}},
      series: example.series.map((series, i) => ({
        name: series.name, type: series.type, data: series.values,
        ...(series.type === "line" ? {symbol: "circle", symbolSize: 6, lineStyle: {type: i === 0 ? "solid" : "dashed"}} : {}),
        ...(series.type === "candlestick" ? {itemStyle: {color: "transparent", color0: palette[3],
          borderColor: palette[0], borderColor0: palette[3]}} : {})
      }))
    }, {notMerge: true});
    const table = document.createElement("table");
    table.createCaption().textContent = example.title;
    const header = table.createTHead().insertRow();
    for (const name of ["Evento", ...example.series.map(series => series.name)]) {
      const th = document.createElement("th");
      th.scope = "col";
      th.textContent = name;
      header.append(th);
    }
    const body = table.createTBody();
    example.labels.forEach((label, i) => {
      const row = body.insertRow();
      const name = document.createElement("th");
      name.scope = "row";
      name.textContent = label;
      row.append(name);
      for (const series of example.series) row.insertCell().textContent = display(series.values[i]);
    });
    values.replaceChildren(table);
  }
  try {
    const theme = localStorage.getItem("mt5x-dashboard-theme");
    if (theme === "light" || theme === "dark") document.documentElement.dataset.theme = theme;
  } catch (error) { /* o tema do sistema continua disponível */ }
  select.disabled = false;
  select.addEventListener("change", render);
  for (const link of document.querySelectorAll("[data-example]")) {
    link.addEventListener("click", () => {
      select.value = link.dataset.example;
      render();
    });
  }
  themeQuery.addEventListener("change", render);
  new ResizeObserver(() => chart.resize()).observe(chartNode);
  render();
})();
