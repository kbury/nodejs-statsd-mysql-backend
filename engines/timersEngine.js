/**
 *
 *
 */
function MSSQLBackendTimersEngine() {
	var self = this;
}


/**
 *
 *
 */
MSSQLBackendTimersEngine.prototype.buildQuerries = function(timers, time_stamp) {

	var querries = [];
  // console.log("timers in engine: ", timers);
  // Iterate on each timer
  try { 
    for(var timerName in timers) {
      var timerValue = timers[timerName];
      console.log(timerName, timerValue);

      if(timerValue.value.length === 0) {
        console.log("-------------------NO VALUES!!!!!!!!!!!!--------------------");
        continue;
      } else {

        for(valueIndex in timerValue.value) {
          console.log("ValueIndex: ", valueIndex);
          console.log("Value: ", timerValue.value[valueIndex]);
          // We insert the raw timers data, you will need to calculate specific stats on the frontend
          var query = "INSERT INTO timers_statistics (timestamp, name, value, username, ip_address, user_agent) VALUES ( " + 
                            time_stamp                      + ", '"
                            + timerName                     + "', "
                            + timerValue.value[valueIndex]  + ", '"
                            + timerValue.userData[0]        + "', '"
                            + timerValue.userData[1]        + "', '"
                            + timerValue.userData[2]        + "' )";
          querries.push(query);
        }
      }
    }
    console.log("Querries: ", querries);
  } catch (ex) {
    console.log("Exception: ", ex);
  } finally {
    return querries;
  }
}


/**
 *
 *
 */
exports.init = function() {
	var instance = new MSSQLBackendTimersEngine();
  return instance;
};