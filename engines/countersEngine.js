/**
 *
 *
 */
function MSSQLBackendCountersEngine() {
	var self = this;
}


/**
 *
 *
 */
MSSQLBackendCountersEngine.prototype.buildQuerries = function(userCounters, time_stamp) {

	var querries = [];
	// Iterate on each userCounter
  for(var userCounterName in userCounters) {
    var counterValue = userCounters[userCounterName];
    if(counterValue.value === 0) {
      continue;
    } else {
      /**********************************************************************
       * Edit following line to custumize where statsd datas are inserted
       *
       * Parameters :
       *    - userCounterName: Counter name
       *    - counterValue: Counter value
       */

      // old strategy
      //  querries.push("insert into `counters_statistics` select "+time_stamp+", '"+userCounterName+"' , if(max(value),max(value),0) + "+counterValue+"  from `counters_statistics`  where if(name = '"+userCounterName+"', 1,0) = 1 ;");

      // new strategy
      var query = "INSERT INTO counters_statistics (timestamp, name, value, username, ip_address, user_agent) VALUES ( " + 
                            time_stamp                  + ", '"
                            + userCounterName           + "', "
                            + counterValue.value        + ", '"
                            + counterValue.userData[0]  + "', '"
                            + counterValue.userData[1]  + "', '"
                            + counterValue.userData[2]  + "' )";
      querries.push(query);
    }
  }

  return querries;
}


/**
 *
 *
 */
exports.init = function() {
	var instance = new MSSQLBackendCountersEngine();
  return instance;
};
