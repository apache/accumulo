/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/* JSLint global definitions */
/*global
    $, sessionStorage, TIMER:true, NAMESPACES:true, refreshNavBar,
    refreshProblems
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
  if (sessionStorage.autoRefresh === 'false') {
    $('.auto-refresh').parent().removeClass('active');
  } else {
    $('.auto-refresh').parent().addClass('active');
  }
  // Initializes the auto refresh on click listener
  $('.auto-refresh').on("click", function () {
    if ($(this).parent().attr('class') === 'active') {
      $(this).parent().removeClass('active');
      sessionStorage.autoRefresh = 'false';
    } else {
      $(this).parent().addClass('active');
      sessionStorage.autoRefresh = 'true';
    }
  });
}

/**
 * Empty function in case there is no refresh implementation
 */
function refresh() {
  console.info('Using default refresh()');
}

/**
 * Global timer that checks for auto refresh status every 5 seconds
 */
TIMER = setInterval(function () {
  if (sessionStorage.autoRefresh === 'true') {
    $('.auto-refresh').parent().addClass('active');
    refresh();
    refreshNavBar();
  } else {
    $('.auto-refresh').parent().removeClass('active');
  }
}, 5000);

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
  if ((big - Math.floor(big)) !== 0) {
    big = big.toFixed(2);
  }

  // If the number is smaller than the base, return the number with no suffix
  if (big < base) {
    return big;
  }

  var exp, val;
  // Finds which suffix to use
  exp = Math.floor(Math.log(big) / Math.log(base));
  // Divides the number by the equivalent suffix number
  val = big / Math.pow(base, exp);
  // Keeps the number to 2 decimal places and adds the suffix
  return val.toFixed(2) + suffixes[exp];
}

/**
 * Converts a number to a size with suffix
 *
 * @param {number} size Number to convert
 * @return {string} Number with suffix added
 */
function bigNumberForSize(size) {
  if (size === null) {
    size = 0;
  }
  return bigNumber(size, SIZE_SUFFIX, 1024);
}

/**
 * Converts a number to a quantity with suffix
 *
 * @param {number} quantity Number to convert
 * @return {string} Number with suffix added
 */
function bigNumberForQuantity(quantity) {
  if (quantity === null) {
    quantity = 0;
  }
  return bigNumber(quantity, QUANTITY_SUFFIX, 1000);
}

/**
 * Formats the timestamp nicely
 */
function dateFormat(timestamp) {
  var date = new Date(timestamp);
  return date.toLocaleString([], {
      timeStyle: 'long',
      dateStyle: 'medium'
    })
    .split(' ').join('&nbsp;');
}

/**
 * Formats the log level as HTML
 */
function levelFormat(level) {
  if (level === 'WARN') {
    return '<span class="label label-warning">' + level + '</span>';
  }
  if (level === 'ERROR' || level === 'FATAL') {
    return '<span class="label label-danger">' + level + '</span>';
  }
  return level;
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
  if (time === 0) {
    return '&mdash;';
  }

  // Obtains the milliseconds, if time is 0, return milliseconds, and units
  ms = time % 1000;
  time = Math.floor(time / 1000);
  if (time === 0) {
    return ms + 'ms';
  }

  // Obtains the seconds, if time is 0, return seconds, milliseconds, and units
  sec = time % 60;
  time = Math.floor(time / 60);
  if (time === 0) {
    return sec + 's' + '&nbsp;' + ms + 'ms';
  }

  // Obtains the minutes, if time is 0, return minutes, seconds, and units
  min = time % 60;
  time = Math.floor(time / 60);
  if (time === 0) {
    return min + 'm' + '&nbsp;' + sec + 's';
  }

  // Obtains the hours, if time is 0, return hours, minutes, and units
  hr = time % 24;
  time = Math.floor(time / 24);
  if (time === 0) {
    return hr + 'h' + '&nbsp;' + min + 'm';
  }

  // Obtains the days, if time is 0, return days, hours, and units
  day = time % 365;
  time = Math.floor(time / 365);
  if (time === 0) {
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
 * Creates a string with the value to sort and the value to display
 * Options are 0 = firstcell left, 1 = right, 2 = center, 3 = left
 *
 * @param {string} index Index for class to use for cell
 * @param {string} sortValue Value used for sorting
 * @param {string} showValue Value to display
 */
function createTableCell(index, sortValue, showValue) {
  var valueClass = ['firstcell left', 'right', 'center', 'left', ''];

  return '<td class="' + valueClass[index] + '" data-value="' + sortValue +
    '">' + showValue + '</td>';
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
 * Performs an ajax reload for the given DataTable
 *
 * @param {DataTable} table DataTable to perform an ajax reload on
 */
function ajaxReloadTable(table) {
  if (table) {
    table.ajax.reload(null, false); // user paging is not reset on reload
  } else {
    console.error('There was an error reloading the given table');
  }
}

/**
 * Performs GET call and builds console logging message from data received
 * @param {string} call REST url called
 * @param {string} sessionDataVar Session storage/global variable to hold REST data
 */
function getJSONForTable(call, sessionDataVar) {
  console.info("Retrieving " + call);

  return $.getJSON(call, function (data) {
    var jsonDataStr = JSON.stringify(data);

    //Handle data to be stored in global variable instead of session storage
    if (sessionDataVar === "NAMESPACES") {
      NAMESPACES = jsonDataStr;
      console.debug("REST GET call to " + call +
        " stored in " + sessionDataVar + " = " + NAMESPACES);
    } else {
      sessionStorage[sessionDataVar] = jsonDataStr;
      console.debug("REST GET request to " + call +
        " stored in sessionStorage." + sessionDataVar + " = " + sessionStorage[sessionDataVar]);
    }
  });
}

/**
 * Performs POST call and builds console logging message if successful
 * @param {string} call REST url called
 * @param {string} callback POST callback to execute, if available
 * @param {boolean} shouldSanitize Whether to sanitize the call 
 */
function doLoggedPostCall(call, callback, shouldSanitize) {

  if (shouldSanitize) {
    // Change plus sign to use ASCII value to send it as a URL query parameter
    call = sanitize(call);
  }

  console.log("POST call to " + call);

  // Make the rest call, passing success function callback
  $.post(call, function () {
    console.debug("REST POST call to " + call + ": success");
    if (callback != null) {
      console.debug("Now calling the provided callback function");
      callback();
    }
  });
}

///// REST Calls /////////////

/**
 * REST GET call for the manager information,
 * stores it on a sessionStorage variable
 */
function getManager() {
  return getJSONForTable('/rest/manager', 'manager');
}

/**
 * REST GET call for the namespaces, stores it on a global variable
 */
function getNamespaces() {
  return getJSONForTable('/rest/tables/namespaces', 'NAMESPACES');
}

/**
 * REST GET call for the tables, stores it on a sessionStorage variable
 */
function getTables() {
  return getJSONForTable('/rest/tables', 'tables');
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
  if (namespaces.indexOf('*') !== -1) {
    return getTables();
  }
  // Convert the list to a string for the REST call
  namespaceList = namespaces.toString();

  return getJSONForTable('/rest/tables/namespaces/' + namespaceList, 'tables');
}

/**
 * REST POST call to clear a specific dead server
 *
 * @param {string} server Dead Server ID
 */
function clearDeadServers(server) {
  doLoggedPostCall('/rest/tservers?server=' + server, null, false);
}

/**
 * REST GET call for the tservers, stores it on a sessionStorage variable
 */
function getTServers() {
  return getJSONForTable('/rest/tservers', 'tservers');
}

/**
 * REST GET call for the tservers, stores it on a sessionStorage variable
 *
 * @param {string} server Server ID
 */
function getTServer(server) {
  return getJSONForTable('/rest/tservers/' + server, 'server');
}

/**
 * REST GET call for the scans, stores it on a sessionStorage variable
 */
function getScans() {
  return getJSONForTable('/rest/scans', 'scans');
}

/**
 * REST GET call for the bulk imports, stores it on a sessionStorage variable
 */
function getBulkImports() {
  return getJSONForTable('/rest/bulkImports', 'bulkImports');
}

/**
 * REST GET call for the server stats, stores it on a sessionStorage variable
 */
function getServerStats() {
  return getJSONForTable('/rest/tservers/serverStats', 'serverStats');
}

/**
 * REST GET call for the recovery list, stores it on a sessionStorage variable
 */
function getRecoveryList() {
  return getJSONForTable('/rest/tservers/recovery', 'recoveryList');
}

/**
 * REST GET call for the participating tablet servers,
 * stores it on a sessionStorage variable
 *
 * @param {string} table Table ID
 */
function getTableServers(tableID) {
  return getJSONForTable('/rest/tables/' + tableID, 'tableServers');
}

/**
 * REST GET call for the logs, stores it on a sessionStorage variable
 */
function getLogs() {
  return getJSONForTable('/rest/logs', 'logs');
}

/**
 * REST POST call to clear logs
 */
function clearLogs() {
  doLoggedPostCall('/rest/logs/clear', refresh, false);
}

/**
 * REST POST call to clear all table problems
 *
 * @param {string} tableID Table ID
 */
function clearTableProblems(tableID) {
  doLoggedPostCall('/rest/problems/summary?s=' + tableID, refresh, true);
}

/**
 * REST POST call to clear detail problems
 *
 * @param {string} table Table ID
 * @param {string} resource Resource for problem
 * @param {string} type Type of problem
 */
function clearDetailsProblems(table, resource, type) {
  doLoggedPostCall('/rest/problems/details?table=' + table + '&resource=' +
    resource + '&ptype=' + type, refresh, true);
}

/**
 * REST GET call for the problems summary,
 * stores it on a sessionStorage variable
 */
function getProblemSummary() {
  return getJSONForTable('/rest/problems/summary', 'problemSummary');
}

/**
 * REST GET call for the problems details,
 * stores it on a sessionStorage variable
 */
function getProblemDetails() {
  return getJSONForTable('/rest/problems/details', 'problemDetails');
}

//// Overview Plots Rest Calls

/**
 * REST GET call for the ingest rate,
 * stores it on a sessionStorage variable
 */
function getIngestRate() {
  return getJSONForTable('/rest/statistics/time/ingestRate', 'ingestRate');
}

/**
 * REST GET call for the scan entries,
 * stores it on a sessionStorage variable
 */
function getScanEntries() {
  return getJSONForTable('/rest/statistics/time/scanEntries', 'scanEntries');
}

/**
 * REST GET call for the ingest byte rate,
 * stores it on a sessionStorage variable
 */
function getIngestByteRate() {
  return getJSONForTable('/rest/statistics/time/ingestByteRate', 'ingestMB');
}

/**
 * REST GET call for the query byte rate, stores it on a sessionStorage variable
 */
function getQueryByteRate() {
  return getJSONForTable('/rest/statistics/time/queryByteRate', 'queryMB');
}

/**
 * REST GET call for the load average, stores it on a sessionStorage variable
 */
function getLoadAverage() {
  return getJSONForTable('/rest/statistics/time/load', 'loadAvg');
}

/**
 * REST GET call for the lookups, stores it on a sessionStorage variable
 */
function getLookups() {
  return getJSONForTable('/rest/statistics/time/lookups', 'lookups');
}

/**
 * REST GET call for the minor compactions,
 * stores it on a sessionStorage variable
 */
function getMinorCompactions() {
  return getJSONForTable('/rest/statistics/time/minorCompactions', 'minorCompactions');
}

/**
 * REST GET call for the major compactions,
 * stores it on a sessionStorage variable
 */
function getMajorCompactions() {
  return getJSONForTable('/rest/statistics/time/majorCompactions', 'majorCompactions');
}

/**
 * REST GET call for the index cache hit rate,
 * stores it on a sessionStorage variable
 */
function getIndexCacheHitRate() {
  return getJSONForTable('/rest/statistics/time/indexCacheHitRate', 'indexCache');
}

/**
 * REST GET call for the data cache hit rate,
 * stores it on a sessionStorage variable
 */
function getDataCacheHitRate() {
  return getJSONForTable('/rest/statistics/time/dataCacheHitRate', 'dataCache');
}

/**
 * REST GET call for the server status, stores it on a sessionStorage variable
 */
function getStatus() {
  return getJSONForTable('/rest/status', 'status');
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
