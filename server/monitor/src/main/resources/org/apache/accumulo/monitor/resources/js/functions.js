/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
"use strict";

// Suffixes for quantity
var QUANTITY_SUFFIX = ['', 'K', 'M', 'B', 'T', 'e15', 'e18', 'e21'];
// Suffixes for size
var SIZE_SUFFIX = ['', 'K', 'M', 'G', 'T', 'P', 'E', 'Z'];

/**
 * Initializes Auto Refresh to false if it is not set,
 * and creates listeners for auto refresh
 */
function setupAutoRefresh() {
  // Sets auto refresh to true or false
  if (!sessionStorage.autoRefresh) {
    sessionStorage.autoRefresh = 'false';
  }
  // Need this to set the initial value for the autorefresh on page load
  if (sessionStorage.autoRefresh == 'false') {
    $('.auto-refresh').parent().removeClass('active');
  } else {
    $('.auto-refresh').parent().addClass('active');
  }
  // Initializes the auto refresh on click listener
  $('.auto-refresh').on("click", function(e) {
    if ($(this).parent().attr('class') == 'active') {
      $(this).parent().removeClass('active');
      sessionStorage.autoRefresh = 'false';
    } else {
      $(this).parent().addClass('active');
      sessionStorage.autoRefresh = 'true';
    }
  });
}

/**
 * Global timer that checks for auto refresh status every 5 seconds
 */
TIMER = setInterval(function() {
  if (sessionStorage.autoRefresh == 'true') {
    $('.auto-refresh').parent().addClass('active');
    refresh();
    refreshNavBar();
  } else {
    $('.auto-refresh').parent().removeClass('active');
  }
}, 5000);

/**
 * Empty function in case there is no refresh implementation
 */
function refresh() {
}

/**
 * Converts a number to a size with suffix
 *
 * @param {number} size Number to convert
 * @return {string} Number with suffix added
 */
function bigNumberForSize(size) {
  if (size === null)
    size = 0;
  return bigNumber(size, SIZE_SUFFIX, 1024);
}

/**
 * Converts a number to a quantity with suffix
 *
 * @param {number} quantity Number to convert
 * @return {string} Number with suffix added
 */
function bigNumberForQuantity(quantity) {
  if (quantity === null)
    quantity = 0;
  return bigNumber(quantity, QUANTITY_SUFFIX, 1000);
}

/**
 * Adds the suffix to the number, converts the number to one close to the base
 *
 * @param {number} big Number to convert
 * @param {array} suffixes Suffixes to use for convertion
 * @param {number} base Base to use for convertion
 * @return {string} The new value with the suffix
 */
function bigNumber(big, suffixes, base) {
  // if the number is a fraction keep to 2 decimal places
  if ((big - Math.floor(big)) !== 0)
    big = big.toFixed(2);
  // If the number is smaller than the base, return the number with no suffix
  if (big < base) {
    return big;
  }
  // Finds which suffix to use
  var exp = Math.floor(Math.log(big) / Math.log(base));
  // Divides the number by the equivalent suffix number
  var val = big / Math.pow(base, exp);
  // Keeps the number to 2 decimal places and adds the suffix
  return val.toFixed(2) + suffixes[exp];
}

/**
 * Formats the timestamp nicely
 */
function dateFormat(timestamp) {
   var date = new Date(timestamp);
   date.toLocaleString().split(' ').join('&nbsp;');
   return date;
}

/**
 * Formats the log level as HTML
 */
function levelFormat(level) {
  if (level === 'WARN') {
    return '<span class="label label-warning">' + level + '</span>';
  } else if (level === 'ERROR' || level === 'FATAL') {
    return '<span class="label label-danger">' + level + '</span>';
  } else {
    return level;
  }
}

/**
 * Converts the time to short number and adds unit
 *
 * @param {number} time Time in microseconds
 * @return {string} The time with units
 */
function timeDuration(time) {
  var ms, sec, min, hr, day, yr;
  ms = sec = min = hr = day = yr = -1;

  time = Math.floor(time);

  // If time is 0 return a dash
  if (time == 0) {
    return '&mdash;';
  }

  // Obtains the milliseconds, if time is 0, return milliseconds, and units
  ms = time % 1000;
  time = Math.floor(time / 1000);
  if (time == 0) {
    return ms + 'ms';
  }

  // Obtains the seconds, if time is 0, return seconds, milliseconds, and units
  sec = time % 60;
  time = Math.floor(time / 60);
  if (time == 0) {
    return sec + 's' + '&nbsp;' + ms + 'ms';
  }

  // Obtains the minutes, if time is 0, return minutes, seconds, and units
  min = time % 60;
  time = Math.floor(time / 60);
  if (time == 0) {
    return min + 'm' + '&nbsp;' + sec + 's';
  }

  // Obtains the hours, if time is 0, return hours, minutes, and units
  hr = time % 24;
  time = Math.floor(time / 24);
  if (time == 0) {
    return hr + 'h' + '&nbsp;' + min + 'm';
  }

  // Obtains the days, if time is 0, return days, hours, and units
  day = time % 365;
  time = Math.floor(time / 365);
  if (time == 0) {
    return day + 'd' + '&nbsp;' + hr + 'h';
  }

  // Obtains the years, if time is 0, return years, days, and units
  yr = Math.floor(time);
  return yr + 'y' + '&nbsp;' + day + 'd';
}

/**
 * Changes + to %2B in the URL
 *
 * @param {string} url URL to sanitize
 */
function sanitize(url) {
  return url.split('+').join('%2B');
}

/**
 * Clears the selected table while leaving the headers
 *
 * @param {string} tableID Table to clear
 */
function clearTableBody(tableID) {
  // JQuery selector to select all rows except for the first row (header)
  $('#' + tableID + ' tbody tr').remove();
}

function createFirstCell(sortValue, showValue) {
  return createTableCell(0, sortValue, showValue);
}

function createRightCell(sortValue, showValue) {
  return createTableCell(1, sortValue, showValue);
}

function createCenterCell(sortValue, showValue) {
  return createTableCell(2, sortValue, showValue);
}

function createLeftCell(sortValue, showValue) {
  return createTableCell(3, sortValue, showValue);
}

/**
 * Creates a row specifying the column span and a message
 *
 * @param {number} col Number of columns
 * @param {string} msg Message to display
 */
function createEmptyRow(col, msg) {
  return '<td class="center" colspan="' + col + '"><i>' + msg + '</i></td>';
}

/**
 * Creates a string with the value to sort and the value to display
 * Options are 0 = firstcell left, 1 = right, 2 = center, 3 = left
 *
 * @param {string} index Index for class to use for cell
 * @param {string} sortValue Value used for sorting
 * @param {string} showValue Value to display
 */
function createTableCell(index, sortValue, showValue) {
  var valueClass = ['firstcell left', 'right', 'center', 'left', ''];

  return '<td class="'+ valueClass[index] + '" data-value="' + sortValue +
      '">' + showValue + '</td>';
}

/**
 * Builds console logging message for REST GET calls
 * @param {string} REST url called
 * @param {object} data returned from REST call
 * @param {string} session storage/global variable to hold REST data
 */
 function createRestGetLog(call, data, sessionDataVar){
    var jsonDataStr = JSON.stringify(data);

    //Handle data to be stored in global variable instead of session storage
    if (sessionDataVar==="NAMESPACES") {
      NAMESPACES = jsonDataStr;
      console.debug("REST GET call to " + call +
       " stored in " + sessionDataVar + " = " + NAMESPACES);
    }
    else {
      sessionStorage[sessionDataVar] = jsonDataStr;
      console.debug("REST GET request to " + call +
        " stored in sessionStorage." + sessionDataVar + " = " + sessionStorage[sessionDataVar]);
    }
 }

///// REST Calls /////////////

/**
 * REST GET call for the manager information,
 * stores it on a sessionStorage variable
 */
function getManager() {
  var call = '/rest/manager';
  console.info("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'manager');
  });
}

/**
 * REST GET call for the namespaces, stores it on a global variable
 */
function getNamespaces() {
  var call = '/rest/tables/namespaces';
  console.info("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'NAMESPACES');
  });
}

/**
 * REST GET call for the tables on each namespace,
 * stores it on a sessionStorage variable
 *
 * @param {array} namespaces Array holding the selected namespaces
 */
function getNamespaceTables(namespaces) {

  // Creates a JSON object to store the tables
  var namespaceList = "";

  /*
   * If the namespace array include *, get all tables, otherwise,
   * get tables from specific namespaces
   */
  if (namespaces.indexOf('*') != -1) {
    return getTables();
  } else {
    // Convert the list to a string for the REST call
    namespaceList = namespaces.toString();

    var call = '/rest/tables/namespaces/' + namespaceList;
    console.info("Retrieving " + call);
    return $.getJSON(call, function(data) {
      createRestGetLog(call, data, 'tables');
    });
  }
}

/**
 * REST GET call for the tables, stores it on a sessionStorage variable
 */
function getTables() {
  var call = '/rest/tables';
  console.info("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'tables');
  });
}

/**
 * REST POST call to clear a specific dead server
 *
 * @param {string} server Dead Server ID
 */
function clearDeadServers(server) {
  var call = '/rest/tservers?server=' + server;
  console.info("Call to " + call);
  $.post(call);
  console.info("REST POST call to " + call);
}

/**
 * REST GET call for the tservers, stores it on a sessionStorage variable
 */
function getTServers() {
  var call = '/rest/tservers';
  console.info("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'tservers');
  });
}

/**
 * REST GET call for the tservers, stores it on a sessionStorage variable
 *
 * @param {string} server Server ID
 */
function getTServer(server) {
  var call = '/rest/tservers/' + server;
  console.info("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'server');
  });
}

/**
 * REST GET call for the scans, stores it on a sessionStorage variable
 */
function getScans() {
  var call = '/rest/scans';
  console.info("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'scans');
  });
}

/**
 * REST GET call for the bulk imports, stores it on a sessionStorage variable
 */
function getBulkImports() {
  var call = '/rest/bulkImports';
  console.info("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'bulkImports');
  });
}

/**
 * REST GET call for the server stats, stores it on a sessionStorage variable
 */
function getServerStats() {
  var call = '/rest/tservers/serverStats';
  console.info("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'serverStats');
  });
}

/**
 * REST GET call for the recovery list, stores it on a sessionStorage variable
 */
function getRecoveryList() {
  var call = '/rest/tservers/recovery';
  console.info("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'recoveryList');;
  });
}

/**
 * REST GET call for the participating tablet servers,
 * stores it on a sessionStorage variable
 *
 * @param {string} table Table ID
 */
function getTableServers(tableID) {
  var call = '/rest/tables/' + tableID;
  console.info("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'tableServers');
  });
}

/**
 * REST GET call for the trace summary, stores it on a sessionStorage variable
 *
 * @param {string} minutes Number of minutes to display trace summary
 */
function getTraceSummary(minutes) {
  var call = '/rest/trace/summary/' + minutes;
  console.info("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'traceSummary');
  });
}

/**
 * REST GET call for the trace type, stores it on a sessionStorage variable
 *
 * @param {string} type Type of the trace
 * @param {string} minutes Number of minutes to display trace
 */
function getTraceOfType(type, minutes) {
  var call = '/rest/trace/listType/' + type + '/' + minutes;
  console.info("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'traceType');
  });
}

/**
 * REST GET call for the trace id, stores it on a sessionStorage variable
 *
 * @param {string} id Trace ID
 */
function getTraceShow(id) {
  var call = '/rest/trace/show/' + id;
  console.info("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'traceShow');
  });
}

/**
 * REST GET call for the logs, stores it on a sessionStorage variable
 */
function getLogs() {
  var call = '/rest/logs';
  console.info("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'logs');
  });
}

/**
 * REST POST call to clear logs
 */
function clearLogs() {
  var call = '/rest/logs/clear';
  $.post(call);
  console.info("REST POST call to " + call);
}

/**
 * REST POST call to clear all table problems
 *
 * @param {string} tableID Table ID
 */
function clearTableProblems(tableID) {
  var call = '/rest/problems/summary?s=' + tableID;
  // Change plus sign to use ASCII value to send it as a URL query parameter
  call = sanitize(call);
  console.info("Call to " + call);
  // make the rest call, passing success function callback
  $.post(call, function () {
    console.info("REST POST call to " + call);
    refreshProblems();
  });
}

/**
 * REST POST call to clear detail problems
 *
 * @param {string} table Table ID
 * @param {string} resource Resource for problem
 * @param {string} type Type of problem
 */
function clearDetailsProblems(table, resource, type) {
  var call = '/rest/problems/details?table=' + table + '&resource=' +
   resource + '&ptype=' + type;
  // Changes plus sign to use ASCII value to send it as a URL query parameter
  call = sanitize(call);
  console.info("Call to " + call);
  // make the rest call, passing success function callback
  $.post(call, function () {
    console.info("REST POST call to " + call);
    refreshProblems();
  });
}

/**
 * REST GET call for the problems summary,
 * stores it on a sessionStorage variable
 */
function getProblemSummary() {
  var call = '/rest/problems/summary';
  console.info("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'problemSummary');
  });
}

/**
 * REST GET call for the problems details,
 * stores it on a sessionStorage variable
 */
function getProblemDetails() {
  var call = '/rest/problems/details';
  console.info("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'problemDetails');
  });
}

/**
 * REST GET call for the replication table,
 * stores it on a sessionStorage variable
 */
function getReplication() {
  var call = '/rest/replication';
  console.info("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'replication');
  });
}

//// Overview Plots Rest Calls
//// Note: console.debug messages may not show by default in some browsers

/**
 * REST GET call for the ingest rate,
 * stores it on a sessionStorage variable
 */
function getIngestRate() {
  var call = '/rest/statistics/time/ingestRate';
  console.debug("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'ingestRate');
  });
}

/**
 * REST GET call for the scan entries,
 * stores it on a sessionStorage variable
 */
function getScanEntries() {
  var call = '/rest/statistics/time/scanEntries';
  console.debug("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'scanEntries');
  });
}

/**
 * REST GET call for the ingest byte rate,
 * stores it on a sessionStorage variable
 */
function getIngestByteRate() {
  var call = '/rest/statistics/time/ingestByteRate';
  console.debug("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'ingestMB');
  });
}

/**
 * REST GET call for the query byte rate, stores it on a sessionStorage variable
 */
function getQueryByteRate() {
  var call = '/rest/statistics/time/queryByteRate';
  console.debug("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'queryMB');
  });
}

/**
 * REST GET call for the load average, stores it on a sessionStorage variable
 */
function getLoadAverage() {
  var call = '/rest/statistics/time/load';
  console.debug("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'loadAvg');
  });
}

/**
 * REST GET call for the lookups, stores it on a sessionStorage variable
 */
function getLookups() {
  var call = '/rest/statistics/time/lookups';
  console.debug("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'lookups');
  });
}

/**
 * REST GET call for the minor compactions,
 * stores it on a sessionStorage variable
 */
function getMinorCompactions() {
  var call = '/rest/statistics/time/minorCompactions';
  console.debug("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'minorCompactions');
  });
}

/**
 * REST GET call for the major compactions,
 * stores it on a sessionStorage variable
 */
function getMajorCompactions() {
  var call = '/rest/statistics/time/majorCompactions';
  console.debug("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'majorCompactions');
  });
}

/**
 * REST GET call for the index cache hit rate,
 * stores it on a sessionStorage variable
 */
function getIndexCacheHitRate() {
  var call = '/rest/statistics/time/indexCacheHitRate';
  console.debug("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'indexCache');
  });
}

/**
 * REST GET call for the data cache hit rate,
 * stores it on a sessionStorage variable
 */
function getDataCacheHitRate() {
  var call = '/rest/statistics/time/dataCacheHitRate';
  console.debug("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'dataCache');
  });
}

/**
 * REST GET call for the server status, stores it on a sessionStorage variable
 */
function getStatus() {
  var call = '/rest/status';
  console.debug("Retrieving " + call);
  return $.getJSON(call, function(data) {
    createRestGetLog(call, data, 'status');
  });
}

/*
 * Jquery call to clear all data from cells of a table
 */
function clearAllTableCells(tableId) {
    console.log("Clearing all table cell data for " + tableId);
    $("#" + tableId + " > tbody > tr > td").each(function () {
        $(this).text("");
    });
}
