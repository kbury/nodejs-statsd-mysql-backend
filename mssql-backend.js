///////////////////////////////////////////////////////////////////////////////////
//  NodeJS Statsd MSSQL Backend 1.0.0
// ------------------------------------------------------------------------------
//
// Authors: Nicolas FRADIN, Damien PACAUD
// Date: 31/10/2012
//
///////////////////////////////////////////////////////////////////////////////////

var _ = require('lodash'),
    mssql = require('mssql'),
    util = require('util'),
    fs = require('fs'),
    path = require('path'),
    sequence = require('sequence').Sequence.create();

var STATSD_PACKETS_RECEIVED = "statsd.packets_received";
var STATSD_BAD_LINES = "statsd.bad_lines_seen";


/**
 * Backend Constructor
 *
 * Example config :
 *
  mssql: { 
   server: "localhost\MSSQL2012",
   user: "root", 
   password: "root", 
   database: "statsd_db"
  }
 *
 * @param startupTime
 * @param config
 * @param emmiter
 */
function StatdMSSQLBackend(startupTime, config, emitter) {
  var self = this;
  self.config = config.mssql || {};
  self.config.password = config.mssql.password || process.env.STATSD_PASSWORD;

  console.log(self.config);
  self.engines = {
    counters: [],
    gauges: [],
    timers: [],
    sets: []
  };

  // Verifying that the config file contains enough information for this backend to work  
  if(!this.config.server || !this.config.database || !this.config.user || !this.config.password) {
    console.log("You need to specify at least server, database, user and password for this MSSQL backend");
    process.exit(-1);
  }

  // Set backend path
  for(var backend_index in config.backends) {
    var currentBackend = config.backends[backend_index];
    if(currentBackend.indexOf('mssql-backend.js') > -1) {
      self.config.backendPath = path.join(__dirname, '/');
    }
  }

  //Default tables
  if(!this.config.tables) {
    this.config.tables = {counters: ["counters_statistics"], gauges: ["gauges_statistics"], timers:["timers_statistics"],sets:["sets_statistics"]};
  }

  // Default engines
  if(!self.config.engines) {
    self.config.engines = {
      counters: ["engines/countersEngine.js"],
      gauges: ["engines/gaugesEngine.js"],
      timers: ["engines/timersEngine.js"],
      sets: ["engines/setsEngine.js"]
    };
  }
  
  // Synchronous sequence
  sequence.then(function( next ) {

    // Check if tables exists
    self.checkDatabase(function(err) {
      if(err) {
        console.log('Database check failed ! Exit...');
        process.exit(-1);
      } else {
        console.log('Database is valid.');
        next();
      }
    });

  }).then(function( next ) {
    process.stdout.write('Loading MSSQL backend engines...');
    // Load backend engines
    self.loadEngines(function(err) {
      if(err) {
        process.stdout.write("[FAILED]\n");
        console.log(err);
      }
      process.stdout.write("[OK]\n");
      next();
    });

  }).then(function( next ) {
    // Attach events
    emitter.on('flush', function(time_stamp, metrics) { self.onFlush(time_stamp, metrics); } );
    emitter.on('status', self.onStatus );

    console.log("Statsd MSSQL backend is loaded.");
  });
 
}


/**
 * Load MSSQL Backend Query Engines
 *
 */
StatdMSSQLBackend.prototype.loadEngines = function(callback) {
  var self = this;

  // Iterate on each engine type defined in configuration
  for(var engineType in self.config.engines) {
    var typeEngines = self.config.engines[engineType];

    // Load engines for current type
    for(var engineIndex in typeEngines) {
      // Get current engine path
      var enginePath = typeEngines[engineIndex];

      // Load current engine
      var currentEngine = require(self.config.backendPath + enginePath).init();
      if(currentEngine === undefined) {
        callback("Unable to load engine '" + enginePath + "' ! Please check...");
      }
      // Add engine to MSSQL Backend engines
      self.engines[engineType].push(currentEngine);
    }
  }

  callback();
}


/**
 * Open MSSQL connection
 *
 * @return boolean Indicates if connection succeed
 */
StatdMSSQLBackend.prototype.openMSSqlConnection = function() {
  var self = this;

  // Create MSSQL connection
  self.sqlConnection = new mssql.Connection(self.config);
  
  self.sqlConnection.on('error', function(error) {
    console.log("Unable to connect to MSSQL database ! Please check...");
    process.exit(-1);
  });

  self.sqlConnection.on('connect', function() {
    console.log("SQL connection established successfully");
  });

  self.sqlConnection.on('close', function() {
    console.log("Sql connection terminated successfully");
  });

  console.log("Connecting...");
  return self.sqlConnection.connect();
}


/**
 * Close MSSQL connection
 *
 */
StatdMSSQLBackend.prototype.closeMSSqlConnection = function() {
  var self = this;
  self.sqlConnection.close(function(error) {
    if(error){
      console.log("There was an error while trying to close DB connection : " + util.inspect(error));
      //Let's make sure that socket is destroyed
      self.sqlConnection.destroy();
    }
  });

  return;
}



/**
 * Check if required tables are created. If not create them.
 *
 */
StatdMSSQLBackend.prototype.checkDatabase = function(callback) {
  var self = this;

  console.log("Checking database...");

  self.openMSSqlConnection()
    .then(function() {
      console.log("Connected...");

      var tables = self.config.tables

      // Count stats types
      var typesCount = 0;
      for(var statType in tables) { typesCount++; }

      // Iterate on each stat type (counters, gauges, ...)
      var statTypeIndex = 0;
      for(var statType in tables) {

        // Get tables for current stat type
        var typeTables = tables[statType];

        // Count tables for current type
        var tablesCount = 0;
        for(var table_index in typeTables) { tablesCount++; }

        // Check if tables exists for current type
        self.checkIfTablesExists(statTypeIndex, typeTables, tablesCount, 0, function(type_index, err) {
          if(err) {
            callback(err);
          }

          // If all types were parsed, call the callback method
          if(type_index == typesCount-1) {
            callback();
          }
        });

        statTypeIndex++;
      }

    });
}



/**
 * Check if a table exists in database. If not, create it.
 */
StatdMSSQLBackend.prototype.checkIfTablesExists = function(type_index, tables_names, size, startIndex, callback) {
  var self = this;
  
  console.log("checkIfTablesExists");
  var request = self.sqlConnection.request();
  request.query("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'" + tables_names[startIndex] + "';", function(err, recordsets, returnValue) {
    if(err) {
      console.log("Error getting table names: ", err);
      callback(err);
    }

    // If table wasn't found
    if(recordsets === void 0 || recordsets.length == 0) {

      console.log("Table '" + tables_names[startIndex] + "' was not found !");

      // Create table
      self.createTable(tables_names[startIndex], function(err) {
        if(err) {
          callback(type_index, err);
        }

        if(startIndex == size - 1) {
          // If all tables were created for this type, call the callback method
          callback(type_index);
        } 
        else {
          // Else iterate on the next table to create
          self.checkIfTablesExists(type_index, tables_names, size, startIndex+1, callback);
        }
      });

    } 

    // If table was found in database
    else {
      console.log("Table '" + tables_names[startIndex] + "' was found.");

      if(startIndex == size-1){
        // If all tables were created for this type, call the callback method
        callback(type_index);
      } 
      else {
        // Else iterate on the next table to create
        self.checkIfTablesExists(type_index, tables_names, size, startIndex+1, callback)
      }
    }

  });

}



/**
 * Create a table from corresponding sql script file
 */
StatdMSSQLBackend.prototype.createTable = function(table_name, callback) {
  var self = this;

  // Try to read SQL file for this table
  var sqlFilePath = path.resolve(self.config.backendPath, 'tables', table_name + '.sql');
  fs.readFile(sqlFilePath, 'utf8', function (err,data) {
    if (err) {
      console.log("Unable to read file: '" + sqlFilePath + "' !");
      callback(err);
    }

    // Split querries
    // Remove comment lines, as they break the split
    data = data.replace(/^--.*$/mg, "");
    var querries = data.split("$$");

    // Prepare querries
    var queuedQuerries = "";
    for(var queryIndex in querries) {
      var query = querries[queryIndex];
      if(query.trim() == "") continue;
      queuedQuerries += query;

      if(queuedQuerries[queuedQuerries.length-1] !== ";") {
        queuedQuerries += ";";
      }
    }

    // Execute querries
    var request = self.sqlConnection.request();
    request.query(queuedQuerries, function(err, recordsets, returnValue) {
      if(err) {
        console.log("Unable to execute query: '" + queuedQuerries + "' for table '" + table_name + "' !");
        console.log(err);
        callback(err);
      } 
      console.log("Table '" + table_name + "' was created with success.");
      callback();
    });
    
  });
}



/**
 * Method executed when statsd flush received datas
 *
 * @param time_stamp
 * @param metrics
 */
StatdMSSQLBackend.prototype.onFlush = function(time_stamp, metrics) {
  var self = this;

  var counters = _.clone(metrics['counters']);
  var timers = _.clone(metrics['timers']);  
  var gauges = _.clone(metrics['gauges']);
  var sets = _.clone(metrics['sets']);
  var pctThreshold = _.clone(metrics['pctThreshold']);

  // console.log("METRICS : \n " + util.inspect(metrics) + "\n ===========================");
  //
  
  // Open MSSQL connection
  self.openMSSqlConnection()
    .then(function() {

      // Handle statsd counters
      self.handleCounters(counters, time_stamp);

      // Handle statsd gauges
      // self.handleMeasures(gauges, self.engines.gauges, time_stamp);
      
      // Handle statsd timers
      self.handleMeasures(timers, self.engines.timers, time_stamp);
      
      // Handle stastd sets
      // self.handleMeasures(sets, self.engines.sets, time_stamp);

      // Close MSSQL Connection
      self.closeMSSqlConnection();

    });  
}



/**
 * Handle and process received counters 
 * 
 * @param _counters received counters
 * @param time_stamp flush time_stamp 
 */
StatdMSSQLBackend.prototype.handleCounters = function(counters, time_stamp) {
  var self = this;

  var packets_received = parseInt(counters[STATSD_PACKETS_RECEIVED]);
  var bad_lines_seen = parseInt(counters[STATSD_BAD_LINES]);

  if(packets_received > 0) {
    // Get userCounters for this flush
    var userCounters = self.getUserCounters(counters);
    self.handleMeasures(userCounters, self.engines.counters, time_stamp);
  }

}


/**
 * Handle and process received counters 
 * 
 * @param measures received measures
 * @param { measureEngines } [measureEngines] [engines ]
 * @param time_stamp flush time_stamp 
 */
StatdMSSQLBackend.prototype.handleMeasures = function(measures, measureEngines, time_stamp) {
  var self = this;
  
  var measuresSize = 0
  for(var m in measures) { measuresSize++; }

  // If measures received
  if(measuresSize > 0) {
    console.log("Measures received !");
    // console.log("Measures = " + util.inspect(measures));

    var querries = [];

    //////////////////////////////////////////////////////////////////////
    // Call buildQuerries method on each measure engine
    for(var measureEngineIndex in measureEngines) {
      console.log("engineIndex = " + measureEngineIndex);
      var measureEngine = measureEngines[measureEngineIndex];

      // Add current engine querries to querries list
      var engineQuerries = measureEngine.buildQuerries(measures, time_stamp);
      querries = querries.concat(engineQuerries);

      // Insert data into database every 100 query
      if(querries.length >= 100) {
        // Execute querries
        self.executeQuerries(querries);
        querries = [];
      }

    }

    if(querries.length > 0) {
      // Execute querries
      self.executeQuerries(querries);
      querries = [];
    }
  }
}


/**
 * MISSING DOCUMENTATION 
 * 
 * @param sqlQuerries
 */
StatdMSSQLBackend.prototype.executeQuerries = function(sqlQuerries) {
  
  var self = this;

  for(var i = 0 ; i < sqlQuerries.length ; i++){
    console.log("Query " + i + " : " + sqlQuerries[i]);
    var request = self.sqlConnection.request();
    request.query(sqlQuerries[i], function(err, recordsets) {
      if(!err) {
        console.log(" -> Query [SUCCESS]");
      }
      else {
        //TODO : add better error handling code
        console.log(" -> Query [ERROR] ", util.inspect(err)); 
      }
    });  
  }

}



/**
 *
 *
 */
StatdMSSQLBackend.prototype.getUserCounters = function(_counters) {
  var userCounters = {};
  for(var counterName in _counters) {
    var counterNameParts = counterName.split('.');
    if(counterNameParts[0] !== "statsd") {
      userCounters[counterName] = _counters[counterName];
    }
  }
  return userCounters;
}


/**
 *
 * @param error
 * @param backend_name
 * @param stat_name
 * @param stat_value
 */
StatdMSSQLBackend.prototype.onStatus = function(error, backend_name, stat_name, stat_value) {

}


exports.init = function(startupTime, config, events) {
  var instance = new StatdMSSQLBackend(startupTime, config, events);
  return true;
};
