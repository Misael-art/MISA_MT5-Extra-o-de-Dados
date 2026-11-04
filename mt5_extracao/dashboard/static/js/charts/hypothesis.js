(function () {
  "use strict";
  window.MT5XCharts = window.MT5XCharts || {};
  const NUMBER = new Intl.NumberFormat("pt-BR", {minimumFractionDigits: 2, maximumFractionDigits: 2});
  const INTEGER = new Intl.NumberFormat("pt-BR");
  // Anéis concêntricos: amostra no centro, lift no anel do meio, IC no externo.
  // Nenhum anel é preenchido proporcionalmente: lift não tem teto e IC não é probabilidade de lucro.
  window.MT5XCharts.hypothesis = function (element, section, colors) {
    const rings = Object.fromEntries((section.visual.rings || []).map(ring => [ring.ring, ring]));
    if (!rings.sample) return null;
    let palette = colors;
    const chart = echarts.init(element, null, {renderer: "svg"});
    function ring(name, radius, stroke, dashed) {
      return {type: "pie", name, silent: true, radius, center: ["22%", "50%"], label: {show: false},
        labelLine: {show: false}, emphasis: {disabled: true},
        data: [{value: 1, itemStyle: {color: stroke.fill, opacity: stroke.opacity,
          borderColor: stroke.border, borderWidth: dashed ? 1.5 : 0, borderType: dashed ? "dashed" : "solid"}}]};
    }
    function draw() {
      const width = Math.max(140, element.clientWidth * 0.52 - 12);
      const lift = rings.lift?.value, confidence = rings.confidence?.value;
      const liftText = lift === null || lift === undefined ? "lift não informado"
        : "lift " + NUMBER.format(lift) + "× · 1,00× = taxa-base" + (lift >= 1 ? " (acima)" : " (abaixo)");
      const ciText = confidence
        ? "IC " + Math.round(confidence.level * 100) + "% [" + NUMBER.format(confidence.low) + "; "
          + NUMBER.format(confidence.high) + "] · " + confidence.method
        : "Confiança não informada";
      chart.setOption({animation: false, aria: {enabled: true, label: {description:
        "Hipótese " + section.title + ". Amostra n = " + INTEGER.format(rings.sample.value) + ". " + liftText + ". " + ciText + "."}},
        series: [
          ring("confiança", ["50%", "62%"], confidence
            ? {fill: palette.accent, opacity: 0.35, border: palette.accent}
            : {fill: palette.surface, opacity: 1, border: palette.muted}, !confidence),
          ring("lift", ["34%", "46%"], lift === null || lift === undefined
            ? {fill: palette.surface, opacity: 1, border: palette.muted}
            : {fill: lift >= 1 ? palette.accent : palette.muted, opacity: 0.8, border: palette.line},
            lift === null || lift === undefined)],
        graphic: [
          {type: "text", left: "22%", top: "middle", style: {text: "n\n" + INTEGER.format(rings.sample.value),
            fill: palette.ink, fontSize: 18, fontWeight: 700, align: "center", verticalAlign: "middle", x: -18}},
          {type: "text", left: "46%", top: "28%", style: {text: "Amostra (centro): " + INTEGER.format(rings.sample.value)
            + " eventos", fill: palette.ink, fontSize: 13, width, overflow: "break"}},
          {type: "text", left: "46%", top: "46%", style: {text: "Anel do meio: " + liftText, fill: palette.ink, fontSize: 13, width, overflow: "break"}},
          {type: "text", left: "46%", top: "64%", style: {text: "Anel externo: " + ciText,
            fill: confidence ? palette.ink : palette.muted, fontSize: 13, width, overflow: "break"}}]}, true);
    }
    draw();
    return {resize: () => {chart.resize(); draw();}, dispose: () => chart.dispose(),
      mt5xTheme: value => {palette = value; draw();}};
  };
}());
