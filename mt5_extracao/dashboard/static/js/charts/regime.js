(function () {
  "use strict";
  window.MT5XCharts = window.MT5XCharts || {};
  window.MT5XCharts.regime = function (element, section, colors) {
    let palette = colors, startHour = Math.max(0, Math.min(18, section.visual.current_hour - 2));
    const days = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sáb", "Dom"];
    const cells = new Map(section.visual.cells.map(cell => [cell.day + ":" + cell.hour, cell]));
    const fragment = document.querySelector("#regime-controls").content.cloneNode(true);
    const controls = fragment.querySelector(".regime-context");
    const readout = controls.querySelector(".cell-readout");
    controls.querySelector(".regime-intensity").textContent = section.visual.intensity_label + " · não é probabilidade";
    element.before(controls);
    const chart = echarts.init(element, null, {renderer: "svg"});
    let selected = [section.visual.current_hour, section.visual.current_day];
    function describe(hour, day) {
      const cell = cells.get(day + ":" + hour);
      readout.textContent = days[day] + " · " + hour + "h UTC · " + (cell
        ? cell.label + " · concordância " + (cell.intensity === null ? "não informada" : cell.intensity.toFixed(2))
          + " · observado em " + cell.observation_time
        : "sem registro neste recorte");
    }
    function draw() {
      const width = element.clientWidth, windowHours = width < 750 ? 6 : 24;
      const left = windowHours === 24 ? 0 : startHour, right = left + windowHours - 1;
      controls.querySelector(".hours-label").textContent = left + "h–" + right + "h UTC";
      controls.querySelector('[data-step="-6"]').disabled = left === 0;
      controls.querySelector('[data-step="6"]').disabled = right === 23;
      controls.querySelector(".regime-hours").hidden = windowHours === 24;
      const data = days.flatMap((_, day) => Array.from({length: 24}, (_, hour) =>
        [hour, day, cells.get(day + ":" + hour)?.intensity ?? null]));
      chart.setOption({animation: false, aria: {enabled: true, label: {description:
        "Regime por dia e hora UTC. Cada célula mostra o último registro, sua concordância e a hora do corte."}},
        grid: {left: 42, right: 10, top: 18, bottom: 28},
        xAxis: {type: "category", data: Array.from({length:24}, (_, i) => i), min: left, max: right,
          axisLabel: {interval: 0, color: palette.muted, formatter: value => value + "h"},
          axisLine: {lineStyle: {color: palette.line}}},
        yAxis: {type: "category", data: days, inverse: true, axisLabel: {color: palette.muted},
          axisLine: {show: false}, axisTick: {show: false}},
        series: [{type: "custom", encode: {x: 0, y: 1}, data, renderItem: (params, api) => {
          const hour = api.value(0), day = api.value(1);
          if (hour < left || hour > right) return;
          const cell = cells.get(day + ":" + hour), point = api.coord([hour, day]), size = api.size([1, 1]);
          const current = hour === section.visual.current_hour && day === section.visual.current_day;
          const label = cell ? cell.label + "\n" + (cell.intensity === null ? "Sem medida" : cell.intensity.toFixed(2)) : "Sem\nregistro";
          return {type: "group", children: [
            {type: "rect", shape: {x: point[0]-size[0]/2+1, y: point[1]-size[1]/2+1, width: size[0]-2, height: size[1]-2},
              style: {fill: cell && cell.intensity !== null ? palette.accent : palette.surface,
                fillOpacity: cell && cell.intensity !== null ? 0.12 + cell.intensity*0.5 : 1,
                stroke: current ? palette.accent : palette.line, lineWidth: current ? 3 : 0.5,
                shadowColor: current ? palette.accent : "transparent", shadowBlur: current ? 4 : 0}},
            {type: "text", style: {x:point[0], y:point[1], text: (current ? "Corte\n" : "") + label,
              align:"center", verticalAlign:"middle", fill:palette.ink, fontSize:windowHours===24?8:10, lineHeight:12}}
          ]};
        }}]}, true);
      describe(...selected);
    }
    controls.querySelectorAll("button[data-step]").forEach(button => button.addEventListener("click", () => {
      startHour = Math.max(0, Math.min(18, startHour + Number(button.dataset.step))); draw();
    }));
    chart.on("click", event => {if (event.value) {selected=[event.value[0],event.value[1]]; describe(...selected);}});
    element.addEventListener("keydown", event => {
      if (!["ArrowLeft", "ArrowRight", "ArrowUp", "ArrowDown"].includes(event.key)) return;
      event.preventDefault();
      selected[0] = Math.max(0, Math.min(23, selected[0] + (event.key==="ArrowRight"?1:event.key==="ArrowLeft"?-1:0)));
      selected[1] = Math.max(0, Math.min(6, selected[1] + (event.key==="ArrowDown"?1:event.key==="ArrowUp"?-1:0)));
      if (selected[0]<startHour || selected[0]>startHour+5) startHour=Math.max(0,Math.min(18,selected[0]-2));
      draw();
    });
    draw();
    return {resize: () => {chart.resize(); draw();}, dispose: () => {chart.dispose(); controls.remove();},
      mt5xTheme: value => {palette=value; draw();}};
  };
}());
