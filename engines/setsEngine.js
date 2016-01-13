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
      var setCount = sets[setName].value.values().length;
      if(setCount === 0) {
        continue;
      } else {
          querries.push("INSERT INTO sets_statistics (timestamp, name, value) VALUES (" + time_stamp + ",'" + setName + "'," + setCount + ");");  
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