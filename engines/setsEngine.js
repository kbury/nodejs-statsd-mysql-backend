/**
 *
 *
 */
function MSSQLBackendSetsEngine() {
	var self = this;
}


/**
 *
 *
 */
MSSQLBackendSetsEngine.prototype.buildQuerries = function(sets, time_stamp) {

	var querries = [];
	 // Iterate on each gauge
    for(var setName in sets) {
      var set = sets[setName];
      for(var username in set) {
        var setCount = set[username].value.values().length;
        if(setCount === 0) {
          continue;
        } else {
          var query = "INSERT INTO sets_statistics (timestamp, name, value, username, ip_address, user_agent) VALUES (" +
                        time_stamp                      + ", '"
                        + setName                       + "',"
                        + setCount                      + ", '"
                        + username                      + "', '"
                        + timerValue.userData[0]        + "', '"
                        + timerValue.userData[1]        + "' )";
          querries.push(query);
        }
      }
    }
    return querries;
}


/**
 *
 *
 */
exports.init = function() {
	var instance = new MSSQLBackendSetsEngine();
  return instance;
};