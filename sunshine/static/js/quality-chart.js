(function(){

    initQualityChart('quality_chart');

})()

function initQualityChart(el) {
  $('#' + el).highcharts({
      chart: {
          type: 'bar'
      },
      title: {
          text: null
      },
      xAxis: {
        title: null,
          labels: {
            enabled: false
          }
      },
      yAxis:{
          title: null,
          min: 1989,
          max: 2015,
          labels: {
            formatter: function() { return parseInt(this.value); }
          }
      },
      plotOptions: {
          series: {
              stacking: 'true'
          }
      },
      tooltip: {
        borderColor: "#ccc",
        formatter: function() {
          return this.series.name;
        }
      },
      legend: { reversed: true },
      series: [
        {
          name: '2000 on: Electronic filings',
          data: [ 15 ],
          color: "#43ac6a",
        },
        {
          name: '1999: Incomplete',
          data: [ 1 ],
          color: "#d9edf7"
        },
        {
          name: '1994 - 1999: Manually entered',
          data: [ 5 ],
          color: "#43ac6a"
        }, 
        {
          name: '1989 - 1994: Bad entries',
          data: [ 1994 ],
          color: "#d9edf7"
        }
      ]
  });
}