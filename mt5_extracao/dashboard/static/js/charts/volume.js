(function () {
  "use strict";
  window.MT5XCharts = window.MT5XCharts || {};
  window.MT5XCharts.volume = function (element, section, colors) {
    const series = section.visual.series, baseline = section.visual.baseline;
    let palette = colors, selected = series.length - 1;
    const context = document.querySelector("#volume-context").content.cloneNode(true).querySelector("div");
    const readout = context.querySelector(".volume-readout");
    element.before(context);
    const chart = echarts.init(element, null, {renderer: "svg"});
    const format = v => v === null ? "ausente" : new Intl.NumberFormat("pt-BR", {maximumFractionDigits: 2}).format(v);
    function describe() {
      const point = series[selected];
      readout.textContent = point.time + " · " + format(point.value) + "× · minuto da sessão "
        + (point.minute_of_session ?? "não informado") + " · sessões de referência " + format(point.reference_sessions);
    }
    function draw() {
      chart.setOption({animation: false, aria: {enabled: true, label: {description:
        section.title + ". RVOL em múltiplos da mediana. Referência 1×. Valores em texto junto ao gráfico; setas percorrem registros."}},
        grid: {left: 42, right: 18, top: 34, bottom: 52},
        useUTC: true,
        xAxis: {type: "time",
          axisLabel: {color: palette.muted, hideOverlap: true, formatter: v => {
            const stamp = new Date(v).toISOString(); return stamp.slice(5, 10) + "\n" + stamp.slice(11, 16);
          }},
          axisLine: {lineStyle: {color: palette.line}}},
        yAxis: {type: "value", min: 0, axisLabel: {color: palette.muted, formatter: v => v + "×"},
          splitLine: {lineStyle: {color: palette.line}}},
        series: [{type: "line", data: series.flatMap(p => p.break_before
            ? [[p.time, null], [p.time, p.value]] : [[p.time, p.value]]), connectNulls: false, smooth: false,
          showSymbol: series.length < 40, symbolSize: 5, lineStyle: {color: palette.accent, width: 2},
          itemStyle: {color: palette.accent}, areaStyle: {color: palette.accent, opacity: 0.12},
          markLine: {silent: true, symbol: "none", lineStyle: {color: palette.muted, type: "dashed"},
            label: {show: true, position: "insideEndTop", formatter: "Mediana 1×", color: palette.ink},
            data: [{yAxis: baseline}]}},
          {type: "custom", silent: true, data: series.map((p, i) => [Date.parse(p.time), p.value, i]),
            renderItem: (params, api) => {
              const i = api.value(2), prior = series[i - 1], point = series[i];
              if (!prior || point.break_before || prior.value === null || point.value === null ||
                  Math.max(prior.value, point.value) <= baseline) return;
              // Recorte geométrico do segmento em 1×; não cria uma observação.
              let a = api.coord([Date.parse(prior.time), prior.value]), b = api.coord([Date.parse(point.time), point.value]);
              const y = api.coord([Date.parse(point.time), baseline])[1];
              if (prior.value < baseline) a = [a[0] + (b[0]-a[0])*(baseline-prior.value)/(point.value-prior.value), y];
              if (point.value < baseline) b = [a[0] + (b[0]-a[0])*(prior.value-baseline)/(prior.value-point.value), y];
              return {type: "polygon", shape: {points: [a, b, [b[0], y], [a[0], y]]},
                style: {fill: palette.accent, opacity: 0.28}};
            }}]}, true);
      describe();
    }
    chart.on("click", event => {
      const time = event.value?.[0];
      const stamp = typeof time === "number" ? time : Date.parse(time);
      const index = series.findIndex(p => Date.parse(p.time) === stamp);
      if (index >= 0) {selected = index; describe();}
    });
    element.addEventListener("keydown", event => {
      if (!["ArrowLeft", "ArrowRight"].includes(event.key)) return;
      event.preventDefault();
      selected = Math.max(0, Math.min(series.length - 1, selected + (event.key === "ArrowRight" ? 1 : -1)));
      describe();
    });
    draw();
    return {resize: () => chart.resize(), dispose: () => {chart.dispose(); context.remove();},
      mt5xTheme: value => {palette = value; draw();}};
  };
}());
