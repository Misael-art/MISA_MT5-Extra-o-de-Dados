(function () {
  "use strict";
  window.MT5XCharts = window.MT5XCharts || {};
  const NUMBER = new Intl.NumberFormat("pt-BR", {minimumFractionDigits: 2, maximumFractionDigits: 2});
  const PROFIT_FACTOR = new Intl.NumberFormat("pt-BR", {maximumSignificantDigits: 5});
  const DATE = new Intl.DateTimeFormat("pt-BR", {timeZone: "UTC", day: "2-digit", month: "short",
    year: "numeric", hour: "2-digit", minute: "2-digit"});
  function stamp(value) {
    if (!value) return NaN;
    const text = String(value).replace(" ", "T");
    return Date.parse(/(?:Z|[+-]\d\d:\d\d)$/.test(text) ? text : text + "Z");
  }
  const date = value => Number.isFinite(stamp(value)) ? DATE.format(stamp(value)) : "data ausente";
  const number = value => Number.isFinite(value) ? NUMBER.format(value) : "—";

  window.MT5XCharts.validation = function (element, section, colors) {
    const folds = section.visual.folds || [];
    if (!folds.length) return null;
    const bounds = [section.visual.data_start, section.visual.data_end]
      .concat(folds.flatMap(f => [f.is_start, f.is_end, f.oos_start, f.oos_end])).map(stamp).filter(Number.isFinite);
    if (!bounds.length) return null;
    const start = Math.min(...bounds), end = Math.max(start + 60000, ...bounds);
    const costs = section.evidence.costs;
    const hasTrain = folds.some(fold => Number.isFinite(stamp(fold.is_start)) && Number.isFinite(stamp(fold.is_end)));
    let palette = colors;
    element.style.height = Math.max(180, 40 * folds.length + 48) + "px";
    const controls = document.createElement("div");
    controls.className = "validation-controls";
    const selection = document.createElement("select");
    selection.setAttribute("aria-label", "Escolher janela de validação");
    for (const [index, fold] of folds.entries()) {
      const option = document.createElement("option");
      option.value = index;
      option.textContent = "Janela " + fold.window;
      selection.append(option);
    }
    const legend = document.createElement("span");
    legend.textContent = hasTrain ? "Contorno tracejado: treino · bloco: fora da amostra" : "Bloco: fora da amostra";
    controls.append(selection, legend);
    element.before(controls);
    const readout = document.createElement("p");
    readout.className = "validation-readout";
    readout.setAttribute("role", "status");
    element.after(readout);
    const chart = echarts.init(element, null, {renderer: "svg"});
    function describe(index) {
      const fold = folds[index];
      if (!fold) return;
      selection.value = index;
      const pf = fold.oos_profit_factor === null || fold.oos_profit_factor === undefined
        ? "PF não calculável (" + (fold.oos_profit_factor_status || "sem status") + ")"
        : "PF " + PROFIT_FACTOR.format(fold.oos_profit_factor);
      const train = fold.is_start && fold.is_end ? date(fold.is_start) + " – " + date(fold.is_end) : "datas ausentes";
      const costText = costs && costs.state === "RECORDED"
        ? "Comissão por lote e por lado " + number(costs.config.commission_per_lot)
          + " · slippage por execução " + number(costs.config.slippage_points) + " pts"
        : "Custos ausentes no recibo";
      readout.textContent = "Fora da amostra " + date(fold.oos_start) + " – " + date(fold.oos_end)
        + " · " + (fold.oos_trades ?? "—") + " operações · " + pf + " · resultado líquido " + number(fold.oos_net_profit)
        + ". Treino " + train + " · " + (fold.is_trades ?? "—") + " operações. " + costText + ".";
    }
    function draw() {
      chart.setOption({animation: false, useUTC: true, aria: {enabled: true, label: {description:
        "Walk-forward com " + folds.length + " janelas fora da amostra. Selecione uma janela para ler os dados do recibo."}},
        grid: {left: 76, right: 16, top: 12, bottom: 30},
        xAxis: {type: "time", min: start, max: end, axisLabel: {color: palette.muted, hideOverlap: true, fontSize: 12},
          axisLine: {lineStyle: {color: palette.line}}, splitLine: {show: false}},
        yAxis: {type: "category", inverse: true, data: folds.map(f => "Janela " + f.window),
          axisLabel: {color: palette.muted, fontSize: 12}, axisTick: {show: false}, axisLine: {show: false}},
        series: [{type: "custom", data: folds.map((_, i) => [i]),
          renderItem: (params, api) => {
            const row = api.value(0), fold = folds[row];
            const dataStart = stamp(section.visual.data_start), dataEnd = stamp(section.visual.data_end);
            const [x0, y] = api.coord([Number.isFinite(dataStart) ? dataStart : start, row]);
            const [x1] = api.coord([Number.isFinite(dataEnd) ? dataEnd : end, row]);
            const height = api.size([0, 1])[1] * 0.48, positive = fold.oos_net_profit > 0;
            const children = [{type: "rect", shape: {x: x0, y: y - 1, width: x1 - x0, height: 2},
              style: {fill: palette.line}}];
            function range(left, right, style) {
              left = stamp(left);
              right = stamp(right);
              if (!Number.isFinite(left) || !Number.isFinite(right) || right < left) return;
              const [a] = api.coord([left, row]), [b] = api.coord([right, row]);
              children.push({type: "rect", shape: {x: a, y: y - height / 2, width: Math.max(2, b - a), height}, style});
            }
            range(fold.is_start, fold.is_end, {fill: palette.surface, stroke: palette.muted, lineDash: [4, 3]});
            range(fold.oos_start, fold.oos_end, {fill: positive ? palette.accent : palette.muted});
            return {type: "group", children};
          }}]}, true);
    }
    chart.on("click", event => describe(event.dataIndex));
    selection.addEventListener("change", () => describe(Number(selection.value)));
    draw();
    describe(folds.length - 1);
    return {resize: () => {chart.resize(); draw();}, dispose: () => {chart.dispose(); readout.remove(); controls.remove();},
      mt5xTheme: value => {palette = value; draw();}};
  };
}());
