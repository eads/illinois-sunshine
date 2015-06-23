var ChartHelper = {};
ChartHelper.create = function(el, title, sourceTxt, yaxisLabel, data, startDate, pointInterval, dataType, color, start_date) {
  // console.log("rendering to: #chart_" + iteration);
  // console.log("title: " + title);
  // console.log("sourceTxt: " + sourceTxt);
  // console.log("yaxisLabel: " + yaxisLabel);
  // console.log(dataArray);
  // console.log("startDate: " + startDate);
  // console.log("pointInterval: " + pointInterval);
  console.log(start_date);
  
  var seriesData;
  seriesData = [{
    color: color,
    data: data,
    showInLegend: false,
    name: "Raw",
    lineWidth: 2
  }];

  //$("#charts").append("<div class='chart' id='chart_grouping_" + iteration + "'></div>")
  return new Highcharts.Chart({
      chart: {
          renderTo: el,
          type: 'line',
          marginRight: 10,
          marginBottom: 25
      },
      legend: {
        backgroundColor: "#ffffff",
        borderColor: "#cccccc",
        floating: true,
        verticalAlign: "top",
        x: 20,
        y: 170
      },
      credits: { 
        enabled: false 
      },
      title: null,
      xAxis: {
          dateTimeLabelFormats: { year: "%Y" },
          type: "datetime"
      },
      yAxis: {
          title: null,
          min: 0
      },
      plotOptions: {
        line: {
          animation: false
        },
        series: {
          point: {
            events: {
              click: function() {
                var start_date = moment.utc(new Date(this.x)).format('YYYY-MM-DD');
                var end_date = moment.utc(new Date(this.x)).add(7, 'days').format('YYYY-MM-DD');
                console.log(start_date);
                console.log(end_date);
                window.location.href = '/donations?start_date=' + start_date + '&end_date=' + end_date;
              }
            }
          },
          marker: {
            fillColor: color,
            radius: 0,
            states: {
              hover: {
                enabled: true,
                radius: 5
              }
            }
          },
          pointInterval: ChartHelper.pointInterval(pointInterval),  
          pointStart: startDate,
          shadow: false,
          states: {
             hover: {
                lineWidth: 2
             }
          }
        }
      },
      tooltip: {
          crosshairs: true,
          formatter: function() {
            var s = "<strong>" + ChartHelper.toolTipDateFormat(pointInterval, this.x) + "</strong>";
            $.each(this.points, function(i, point) {
              if (dataType == 'percent')
                s += "<br /><span style='color: " + point.series.color + "'>" + point.series.name + ":</span> " + point.y + "%";
              else if (dataType == 'money')
                s += "<br /><span style='color: " + point.series.color + "'>" + point.series.name + ":</span> $" + Highcharts.numberFormat(point.y, 0, '.', ',');
              else
                s += "<br /><span style='color: " + point.series.color + "'>" + point.series.name + ":</span> " + Highcharts.numberFormat(point.y, 0, '.', ',');
            });
            return s;
          },
          shared: true
      },
      series: seriesData
    });
  }

ChartHelper.pointInterval = function(interval) {
  if (interval == "year")
    return 365 * 24 * 3600 * 1000;
  if (interval == "quarter")
    return 3 * 30.4 * 24 * 3600 * 1000;
  if (interval == "month") //this is very hacky. months have different day counts, so our point interval is the average - 30.4
    return 30.4 * 24 * 3600 * 1000;
  if (interval == "week")
    return 7 * 24 * 3600 * 1000;
  if (interval == "day")
    return 24 * 3600 * 1000;
  if (interval == "hour")
    return 3600 * 1000;
  else
    return 1;
}

ChartHelper.toolTipDateFormat = function(interval, x) {
  if (interval == "year")
    return Highcharts.dateFormat("%Y", x);
  if (interval == "quarter")
    return Highcharts.dateFormat("%B %Y", x);
  if (interval == "month")
    return Highcharts.dateFormat("%B %Y", x);
  if (interval == "week")
    return Highcharts.dateFormat("%e %b %Y", x);
  if (interval == "day")
    return Highcharts.dateFormat("%e %b %Y", x);
  if (interval == "hour")
    return Highcharts.dateFormat("%H:00", x);
  else
    return 1;
}

