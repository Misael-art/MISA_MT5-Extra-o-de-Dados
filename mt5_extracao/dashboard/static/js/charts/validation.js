(function () {
  "use strict";
  window.MT5XCharts = window.MT5XCharts || {};
  const NUMBER = new Intl.NumberFormat("pt-BR", {minimumFractionDigits: 2, maximumFractionDigits: 2});
  const DAY = new Intl.DateTimeFormat("pt-BR", {timeZone: "UTC", day: "2-digit", month: "short", year: "numeric"});
  function stamp(value) {
    if (!value) return NaN;
    const text = String(value).replace(" ", "T");
    return Date.parse(/(?:Z|[+-]\d\d:\d\d)$/.test(text) ? text : text + "Z");
  }
  // Uma linha por fold: só a janela fora da amostra gravada. O treino antecede cada OOS,
  // mas suas datas não constam do recibo; nada é desenhado como se fosse conhecido.
  window.MT5XCharts.validation = function (element, section, colors) {
    const folds = section.visual.folds || [];
    if (!folds.length) return null;
    let palette = colors;
    element.style.height = Math.max(180, 46 * folds.length + 70) + "px";
    const readout = document.createElement("p");
    readout.className = "validation-readout";
    readout.setAttribute("role", "status");
    element.after(readout);
    const chart = echarts.init(element, null, {renderer: "svg"});
    // Eixo cobre recibo e todas as janelas OOS: nenhum fold gravado some por estar fora de data_start/end.
    const bounds = [section.visual.data_start, section.visual.data_end]
      .concat(folds.flatMap(f => [f.oos_start, f.oos_end])).map(stamp).filter(Number.isFinite);
    const start = Math.min(...bounds), end = Math.max(...bounds);
    function describe(fold) {
      const pf = fold.oos_profit_factor === null || fold.oos_profit_factor === undefined
        ? "PF não calculável (" + (fold.oos_profit_factor_status || "sem status") + ")" : "PF " + NUMBER.format(fold.oos_profit_factor);
      readout.textContent = "Fold " + fold.window + " · OOS " + DAY.format(stamp(fold.oos_start)) + " – "
        + DAY.format(stamp(fold.oos_end)) + " · " + fold.oos_trades + " operações · " + pf
        + " · resultado líquido " + NUMBER.format(fold.oos_net_profit ?? 0) + " · treino: " + fold.is_trades
        + " operações, datas não gravadas · custos não gravados no recibo";
    }
    function draw() {
      chart.setOption({animation: false, aria: {enabled: true, label: {description:
        "Walk-forward com " + folds.length + " janelas fora da amostra; datas de treino e custos não constam do recibo."}},
        grid: {left: 70, right: 16, top: 26, bottom: 30},
        xAxis: {type: "time", min: start, max: end, axisLabel: {color: palette.muted, hideOverlap: true, fontSize: 12},
          axisLine: {lineStyle: {color: palette.line}}, splitLine: {show: false}},
        yAxis: {type: "category", inverse: true, data: folds.map(f => "Fold " + f.window),
          axisLabel: {color: palette.muted, fontSize: 12}, axisTick: {show: false}, axisLine: {show: false}},
        graphic: [{type: "text", left: 70, top: 4, style: {text: "Faixa cinza: dados do recibo · bloco: janela fora da amostra (OOS)",
          fill: palette.muted, fontSize: 12}}],
        series: [{type: "custom", data: folds.map((f, i) => [i, stamp(f.oos_start), stamp(f.oos_end)]),
          renderItem: (params, api) => {
            const row = api.value(0), fold = folds[row];
            const dataStart = stamp(section.visual.data_start), dataEnd = stamp(section.visual.data_end);
            const [x0, y] = api.coord([Number.isFinite(dataStart) ? dataStart : start, row]);
            const [x1] = api.coord([Number.isFinite(dataEnd) ? dataEnd : end, row]);
            const [a] = api.coord([api.value(1), row]), [b] = api.coord([api.value(2), row]);
            const height = api.size([0, 1])[1] * 0.56, positive = (fold.oos_net_profit ?? 0) > 0;
            const label = "OOS · " + fold.oos_trades + " op · " + (fold.oos_profit_factor === null ? "PF —" : "PF " + NUMBER.format(fold.oos_profit_factor));
            return {type: "group", children: [
              {type: "rect", shape: {x: x0, y: y - 2, width: x1 - x0, height: 4}, style: {fill: palette.line}},
              {type: "rect", shape: {x: a, y: y - height / 2, width: Math.max(2, b - a), height},
                style: {fill: positive ? palette.accent : palette.surface, stroke: positive ? palette.accent : palette.muted,
                  lineWidth: 1, opacity: positive ? 0.8 : 1}},
              {type: "text", style: {x: Math.max(x0 + 4, a - 6), y, text: label, align: "right", verticalAlign: "middle",
                fill: palette.ink, fontSize: 11}, invisible: a - x0 < 120}]};
          }}]}, true);
    }
    chart.on("click", event => describe(folds[event.dataIndex]));
    draw();
    describe(folds[folds.length - 1]);
    return {resize: () => {chart.resize(); draw();}, dispose: () => {chart.dispose(); readout.remove();},
      mt5xTheme: value => {palette = value; draw();}};
  };
}());
