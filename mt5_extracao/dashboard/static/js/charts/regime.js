(function () {
  "use strict";
  window.MT5XCharts = window.MT5XCharts || {};
  const DAYS = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"];
  const DAY_NAMES = ["segunda", "terça", "quarta", "quinta", "sexta", "sábado", "domingo"];
  // Código curto em uma linha: legível em 24 colunas sem reduzir a fonte.
  const CODES = {TREND: "TEN", RANGE: "LAT", TRANSITION: "TRA", UNCERTAIN: "INC"};
  const HUMAN = new Intl.DateTimeFormat("pt-BR", {timeZone: "UTC", weekday: "short", day: "2-digit",
    month: "short", year: "numeric", hour: "2-digit", minute: "2-digit"});

  const DECIMAL = new Intl.NumberFormat("pt-BR", {minimumFractionDigits: 2, maximumFractionDigits: 2});

  function human(iso) {
    return iso ? HUMAN.format(new Date(iso)) + " UTC" : "não informado";
  }
  function age(iso, cutIso) {
    const minutes = Math.round((Date.parse(cutIso) - Date.parse(iso)) / 60000);
    if (!Number.isFinite(minutes)) return "";
    if (minutes < 60) return minutes + " min antes do corte";
    const hours = minutes / 60;
    return hours < 48 ? Math.round(hours) + " h antes do corte" : Math.round(hours / 24) + " dias antes do corte";
  }

  window.MT5XCharts.regime = function (element, section, colors) {
    let palette = colors, startHour = Math.max(0, Math.min(18, section.visual.current_hour - 2));
    const cutIso = section.as_of_utc, latestIso = section.details.latest_observation_time;
    const latest = latestIso ? new Date(latestIso) : null;
    const latestKey = latest ? ((latest.getUTCDay() + 6) % 7) + ":" + latest.getUTCHours() : null;
    const cells = new Map(section.visual.cells.map(cell => [cell.day + ":" + cell.hour, cell]));
    const fragment = document.querySelector("#regime-controls").content.cloneNode(true);
    const controls = fragment.querySelector(".regime-context");
    controls.querySelector(".regime-intensity").textContent = section.visual.intensity_label + " · não é probabilidade";
    controls.querySelector(".regime-cut").textContent = human(cutIso);
    controls.querySelector(".regime-latest").textContent = latestIso
      ? human(latestIso) + " · " + age(latestIso, cutIso) : "sem observação";
    const readout = controls.querySelector(".cell-readout");
    element.before(controls);
    const chart = echarts.init(element, null, {renderer: "svg"});
    let selected = [section.visual.current_hour, section.visual.current_day];

    function describe(hour, day) {
      const cell = cells.get(day + ":" + hour);
      const where = DAY_NAMES[day] + ", " + hour + "h–" + hour + "h59 UTC";
      readout.textContent = "Selecionada: " + where + " · " + (cell
        ? cell.label + " · concordância " + (cell.intensity === null ? "não informada" : DECIMAL.format(cell.intensity))
          + " · último registro em " + human(cell.observation_time) + " (" + age(cell.observation_time, cutIso) + ")"
        : "sem registro neste recorte (ausência, não estado neutro)");
    }

    function draw() {
      const width = element.clientWidth, windowHours = width < 750 ? 6 : 24;
      const left = windowHours === 24 ? 0 : startHour, right = left + windowHours - 1;
      controls.querySelector(".hours-label").textContent = left + "h–" + right + "h UTC";
      controls.querySelector('[data-step="-6"]').disabled = left === 0;
      controls.querySelector('[data-step="6"]').disabled = right === 23;
      controls.querySelector(".regime-hours").hidden = windowHours === 24;
      const data = DAYS.flatMap((_, day) => Array.from({length: 24}, (_, hour) =>
        [hour, day, cells.get(day + ":" + hour)?.intensity ?? null]));
      chart.setOption({animation: false, aria: {enabled: true, label: {description:
        "Regime por dia e hora UTC. Cada célula mostra o último registro daquele dia e hora no recorte; "
        + "contorno contínuo marca a hora do corte, tracejado a seleção e o ponto a última observação."}},
        grid: {left: 42, right: 10, top: 10, bottom: 28},
        xAxis: {type: "category", data: Array.from({length: 24}, (_, i) => i), min: left, max: right,
          axisLabel: {interval: windowHours === 24 && width < 1000 ? 1 : 0, color: palette.muted, fontSize: 12,
            formatter: value => value + "h"},
          axisLine: {lineStyle: {color: palette.line}}},
        yAxis: {type: "category", data: DAYS, inverse: true, axisLabel: {color: palette.muted, fontSize: 12},
          axisLine: {show: false}, axisTick: {show: false}},
        series: [{type: "custom", encode: {x: 0, y: 1}, data, renderItem: (params, api) => {
          const hour = api.value(0), day = api.value(1);
          if (hour < left || hour > right) return;
          const key = day + ":" + hour, cell = cells.get(key);
          const point = api.coord([hour, day]), size = api.size([1, 1]);
          const x = point[0] - size[0] / 2, y = point[1] - size[1] / 2;
          const isCut = hour === section.visual.current_hour && day === section.visual.current_day;
          const isSelected = hour === selected[0] && day === selected[1];
          const measured = cell && cell.intensity !== null;
          // Só a observação da própria hora do corte é atual; registro antigo na célula do corte não ganha destaque.
          const fresh = cell && Date.parse(cutIso) - Date.parse(cell.observation_time) < 3600000;
          const children = [{type: "rect", shape: {x: x + 1, y: y + 1, width: size[0] - 2, height: size[1] - 2},
            style: {fill: measured ? palette.accent : palette.surface,
              fillOpacity: measured ? 0.12 + cell.intensity * 0.5 : 1,
              stroke: palette.line, lineWidth: cell ? 0.5 : 0.25}}];
          if (cell && size[0] >= 26) children.push({type: "text", style: {x: point[0], y: point[1],
            text: CODES[cell.state] || cell.label, align: "center", verticalAlign: "middle", fill: isCut && !fresh ? palette.muted : palette.ink,
            fontSize: 11, fontWeight: isCut && fresh ? 700 : 500}});
          if (key === latestKey) children.push({type: "circle", shape: {cx: x + size[0] - 8, cy: y + 8, r: 3.5},
            style: {fill: palette.ink}});
          if (isCut) children.push({type: "rect", shape: {x: x + 1.5, y: y + 1.5, width: size[0] - 3, height: size[1] - 3},
            style: {fill: "none", stroke: palette.accent, lineWidth: 3}});
          if (isSelected) children.push({type: "rect", shape: {x: x + 4, y: y + 4, width: size[0] - 8, height: size[1] - 8},
            style: {fill: "none", stroke: palette.ink, lineWidth: 1.5, lineDash: [3, 2]}});
          return {type: "group", children};
        }}]}, true);
      describe(...selected);
    }
    controls.querySelectorAll("button[data-step]").forEach(button => button.addEventListener("click", () => {
      startHour = Math.max(0, Math.min(18, startHour + Number(button.dataset.step))); draw();
    }));
    chart.on("click", event => {if (event.value) {selected = [event.value[0], event.value[1]]; draw();}});
    element.addEventListener("keydown", event => {
      if (!["ArrowLeft", "ArrowRight", "ArrowUp", "ArrowDown"].includes(event.key)) return;
      event.preventDefault();
      selected[0] = Math.max(0, Math.min(23, selected[0] + (event.key === "ArrowRight" ? 1 : event.key === "ArrowLeft" ? -1 : 0)));
      selected[1] = Math.max(0, Math.min(6, selected[1] + (event.key === "ArrowDown" ? 1 : event.key === "ArrowUp" ? -1 : 0)));
      if (selected[0] < startHour || selected[0] > startHour + 5) startHour = Math.max(0, Math.min(18, selected[0] - 2));
      draw();
    });
    draw();
    return {resize: () => {chart.resize(); draw();}, dispose: () => {chart.dispose(); controls.remove();},
      mt5xTheme: value => {palette = value; draw();}};
  };
}());
