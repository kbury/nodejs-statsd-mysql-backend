/**
 *
 *
 */
function MSSQLBackendGaugesEngine() {
	var self = this;
}


/**
 *
 *
 */
MSSQLBackendGaugesEngine.prototype.buildQuerries = function(gauges, time_stamp) {

	var querries = [];
	 // Iterate on each gauge
    for(var gaugeName in gauges) {
      var gaugeValue = gauges[gaugeName].value;
      if(gaugeValue === 0) {
        continue;
      } else {
          /**********************************************************************
           * Edit following line to customize where statsd datas are inserted
           *
           * Parameters :
           *    - gaugeName: Gauge name
           *    - gaugeValue: Gauge value
           */
          // This SQL request checks if the last value for this particular gauge is the same as gaugeValue.
          // If it is the same, we do nothing.
          // If it is different, we insert a new line.
          // If gaugeName does not exist in the table, we insert a new line
          // The -678 value, is totally arbitrary, I just assumed that there was never gonna be a gauge with a -678 value. 
          // You can change it to any value not used by your gauges ;)
        querries.push("INSERT INTO gauges_statistics SELECT " + time_stamp + ", '" + gaugeName + "', " + gaugeValue + " FROM dual WHERE " + 
                        "(SELECT IF (MAX(value),MAX(value),-678) FROM gauges_statistics WHERE name = '" + gaugeName + "') = -678 " +
                        + "OR " +
                        "(SELECT value FROM gauges_statistics WHERE name = '" + gaugeName + "' ORDER BY timestamp DESC LIMIT 0,1) <> " + gaugeValue + ";")

      }
    }

    return querries;
}


/**
 *
 *
 */
exports.init = function() {
	var instance = new MSSQLBackendGaugesEngine();
  return instance;
};