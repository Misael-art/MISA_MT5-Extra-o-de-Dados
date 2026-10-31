(function () {
  "use strict";
  window.MT5XCharts = window.MT5XCharts || {};
  window.MT5XCharts.surprise = function (element, section, colors) {
    const series = section.visual.series || [];
    if (!series.some(point => point.value !== null)) return null;
    const chart = echarts.init(element, null, {renderer: "svg"});
    chart.setOption({animation: false, aria: {enabled: true},
      grid: {left: 48, right: 18, top: 32, bottom: 36},
      xAxis: {type: "time", axisLabel: {color: colors.muted, formatter: "{dd}/{MM}", hideOverlap: true},
        axisLine: {lineStyle: {color: colors.line}}},
      yAxis: {type: "value", scale: true, name: section.unit || "", nameTextStyle: {color: colors.muted},
        axisLabel: {color: colors.muted}, splitLine: {lineStyle: {color: colors.line}}},
      tooltip: {trigger: "axis", renderMode: "richText"},
      series: [{type: "line", connectNulls: false, smooth: false, showSymbol: false,
        lineStyle: {color: colors.accent, width: 2},
        data: series.map(point => [point.time, point.value])}]});
    return chart;
  };
}());
