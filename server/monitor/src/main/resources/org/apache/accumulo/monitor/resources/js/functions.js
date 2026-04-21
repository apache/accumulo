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
    $, sessionStorage, TIMER:true, NAMESPACES:true, refreshNavBar
*/
"use strict";

// Suffixes for quantity
var QUANTITY_SUFFIX = ['', 'K', 'M', 'B', 'T', 'e15', 'e18', 'e21'];
// Suffixes for size
var SIZE_SUFFIX = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB'];
const REST_V2_PREFIX = contextPath + 'rest-v2';
const MANAGER_GOAL_STATE_METRIC = 'accumulo.manager.goal.state';

const COMPACTOR_SERVER_PROCESS_VIEW = 'compactorsView';
const COORDINATOR_QUEUE_PROCESS_VIEW = 'coordinatorQueueView';
const GC_SERVER_PROCESS_VIEW = 'gcSummaryView';
const GC_FILE_SERVER_PROCESS_VIEW = 'gcFileView';
const GC_WAL_SERVER_PROCESS_VIEW = 'gcWalView';
const MANAGER_SERVER_PROCESS_VIEW = 'managerssView';
const MANAGER_FATE_SERVER_PROCESS_VIEW = 'managersFateView';
const MANAGER_COMPACTION_SERVER_PROCESS_VIEW = 'managersCompactionView';
const SCAN_SERVER_PROCESS_VIEW = 'sserversView';
const TABLET_SERVER_PROCESS_VIEW = 'tserversView';
const RUNNING_COMPACTIONS_BY_TABLE = 'runningCompactionsByTable';
const RUNNING_COMPACTIONS_BY_GROUP = 'runningCompactionsByGroup';

// Override Length Menu options for dataTables
if ($.fn && $.fn.dataTable) {
  $.extend(true, $.fn.dataTable.defaults, {
    "lengthMenu": [
      [10, 25, 50, 100, -1],
      [10, 25, 50, 100, "All"]
    ]
  });
}

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
 * Converts a size in bytes to a human-readable string with appropriate units.
 *
 * @param {number} size - The size in bytes to be converted.
 * @returns {string} The human-readable string representation of the size.
 */
function bigNumberForSize(size) {
  if (size === 0) {
    return '0B';
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
 * Maps the given activity state to an icon.
 *
 * @param {number|null|undefined} data Raw value, 1 for idle and 0 for active
 * @param {string} type DataTables render type
 * @return {string|number|null|undefined} HTML for display cells, raw data otherwise
 */
function renderActivityState(data, type) {
  if (type !== 'display') {
    return data;
  }
  if (data === null || data === undefined) {
    return '&mdash;';
  }
  if (Number(data) === 1) {
    return '<i class="bi bi-moon-stars-fill text-muted" title="Idle" aria-hidden="true"></i>' +
      '<span class="visually-hidden">Idle</span>';
  }
  return '<i class="bi bi-activity text-primary" title="Active" aria-hidden="true"></i>' +
    '<span class="visually-hidden">Active</span>';
}

/**
 * Maps the given memory state to an icon.
 *
 * @param {number|null|undefined} data Raw value, 1 for low memory and 0 for normal memory
 * @param {string} type DataTables render type
 * @return {string|number|null|undefined} HTML for display cells, raw data otherwise
 */
function renderMemoryState(data, type) {
  if (type !== 'display') {
    return data;
  }
  if (data === null || data === undefined) {
    return '&mdash;';
  }
  if (Number(data) === 1) {
    return '<i class="bi bi-exclamation-triangle-fill text-warning" title="Low memory detected" aria-hidden="true"></i>' +
      '<span class="visually-hidden">Low memory detected</span>';
  }
  return '<i class="bi bi-check-circle-fill text-success" title="Memory normal" aria-hidden="true"></i>' +
    '<span class="visually-hidden">Memory normal</span>';
}

/**
 * Converts the time to short number and adds unit
 *
 * @param {number} time Time in milliseconds
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
 * Gets the manager goal state from the cached manager response, if available.
 *
 * @return {string|null} Manager goal state (CLEAN_STOP, SAFE_MODE, NORMAL) or null
 */
function getManagerGoalStateFromSession() {
  var mgrs = getStoredRows(MANAGER_SERVER_PROCESS_VIEW);
  if (!Array.isArray(mgrs) || mgrs.length === 0) {
    console.debug('No manager data in session storage. Returning null.');
    return null;
  }
  // There could be multiple managers that report different goal states.
  // The goal state is stored in ZK, but it's eventually consistent.
  // Use the lowest value seen as the current state for the Monitor
  var goalState = 10;
  $.each(mgrs, function (index, mgr) {
    var stateVal = mgr[MANAGER_GOAL_STATE_METRIC];
    if (stateVal < goalState) {
      goalState = stateVal;
    }
  });
  switch (goalState) {
  case 0:
    return 'CLEAN_STOP';
  case 1:
    return 'SAFE_MODE';
  case 2:
    return 'NORMAL';
  default:
    console.debug('Manager goal state metric not found');
    return null;
  }
}

/**
 * REST GET call for the namespaces, stores it on a global variable
 */
function getNamespaces() {
  return getJSONForTable(contextPath + 'rest/tables/namespaces', 'NAMESPACES');
}

/**
 * REST GET call for the tables, stores it on a sessionStorage variable
 */
function getTables() {
  return getJSONForTable(contextPath + 'rest/tables', 'tables');
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

  return getJSONForTable(contextPath + 'rest/tables/namespaces/' + namespaceList, 'tables');
}

/**
 * REST POST call to clear a specific dead server
 *
 * @param {string} server Dead Server ID
 */
function clearDeadServers(server) {
  doLoggedPostCall(contextPath + 'rest/tservers?server=' + server, null, false);
}

/**
 * REST GET call for the tservers, stores it on a sessionStorage variable
 */
function getTServers() {
  return getJSONForTable(contextPath + 'rest/tservers', 'tservers');
}

/**
 * REST GET call for the tservers, stores it on a sessionStorage variable
 *
 * @param {string} server Server ID
 */
function getTServer(server) {
  return getJSONForTable(contextPath + 'rest/tservers/' + server, 'server');
}

/**
 * REST GET call for the scans, stores it on a sessionStorage variable
 */
function getScans() {
  return getJSONForTable(contextPath + 'rest/scans', 'scans');
}

/**
 * REST GET call for the bulk imports, stores it on a sessionStorage variable
 */
function getBulkImports() {
  return getJSONForTable(contextPath + 'rest/bulkImports', 'bulkImports');
}

/**
 * REST GET call for the server stats, stores it on a sessionStorage variable
 */
function getServerStats() {
  return getJSONForTable(contextPath + 'rest/tservers/serverStats', 'serverStats');
}

/**
 * REST GET call for the recovery list, stores it on a sessionStorage variable
 */
function getRecoveryList() {
  return getJSONForTable(contextPath + 'rest/tservers/recovery', 'recoveryList');
}

/**
 * REST GET call for the participating tablet servers,
 * stores it on a sessionStorage variable
 *
 * @param {string} table Table ID
 */
function getTableServers(tableID) {
  return getJSONForTable(contextPath + 'rest/tables/' + tableID, 'tableServers');
}

/**
 * REST GET call for the server status, stores it on a sessionStorage variable
 */
function getStatus() {
  return getJSONForTable(contextPath + 'rest/status', 'status');
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

// NEW REST CALLS

/**
 * REST GET call for /problems,
 * stores it on a sessionStorage variable
 */
function getProblems() {
  return getJSONForTable(REST_V2_PREFIX + '/problems', 'problems');
}

/**
 * REST GET call for /lastUpdate,
 * stores it on a sessionStorage variable
 */
function getLastUpdate() {
  return getJSONForTable(REST_V2_PREFIX + '/lastUpdate', 'lastUpdate');
}

/**
 * REST GET call for /tservers/summary/{group},
 * stores it on a sessionStorage variable
 * @param {string} group Group name
 */
function getTserversSummary(group) {
  const url = `${REST_V2_PREFIX}/tservers/summary/${group}`;
  const sessionDataVar = `tserversSummary_${group}`;
  return getJSONForTable(url, sessionDataVar);
}

/**
 * REST GET call for /suggestions,
 * stores it on a sessionStorage variable
 */
function getSuggestions() {
  return getJSONForTable(REST_V2_PREFIX + '/suggestions', 'suggestions');
}

/**
 * REST GET call for /compactors/detail/{group},
 * stores it on a sessionStorage variable
 * @param {string} group Group name
 */
function getCompactorsDetail(group) {
  const url = `${REST_V2_PREFIX}/compactors/detail/${group}`;
  const sessionDataVar = `compactorsDetail_${group}`;
  return getJSONForTable(url, sessionDataVar);
}

/**
 * REST GET call for /stats,
 * stores it on a sessionStorage variable
 */
function getStats() {
  return getJSONForTable(REST_V2_PREFIX + '/stats', 'stats');
}

/**
 * REST GET call for /compactors/summary/{group},
 * stores it on a sessionStorage variable
 * @param {string} group Group name
 */
function getCompactorsSummary(group) {
  const url = `${REST_V2_PREFIX}/compactors/summary/${group}`;
  const sessionDataVar = `compactorsSummary_${group}`;
  return getJSONForTable(url, sessionDataVar);
}

/**
 * REST GET call for /tables/{name}/tablets,
 * stores it on a sessionStorage variable
 * @param {string} name Table name
 */
function getTableTablets(name) {
  const url = `${REST_V2_PREFIX}/tables/${name}/tablets`;
  const sessionDataVar = `tableTablets_${name}`;
  return getJSONForTable(url, sessionDataVar);
}

/**
 * REST GET call for /metrics,
 * stores it on a sessionStorage variable
 */
function getMetrics() {
  return getJSONForTable(REST_V2_PREFIX + '/metrics', 'metrics');
}

/**
 * REST GET call for /gc,
 * stores it on a sessionStorage variable
 */
function getGc() {
  return getJSONForTable(REST_V2_PREFIX + '/gc', 'gc');
}

/**
 * REST GET call for /tservers/detail/{group},
 * stores it on a sessionStorage variable
 * @param {string} group Group name
 */
function getTserversDetail(group) {
  const url = `${REST_V2_PREFIX}/tservers/detail/${group}`;
  const sessionDataVar = `tserversDetail_${group}`;
  return getJSONForTable(url, sessionDataVar);
}

/**
 * REST GET call for /tables,
 * stores it on a sessionStorage variable
 */
function getTables() {
  return getJSONForTable(REST_V2_PREFIX + '/tables', 'tables');
}

/**
 * REST GET call for /groups,
 * stores it on a sessionStorage variable
 */
function getGroups() {
  return getJSONForTable(REST_V2_PREFIX + '/groups', 'groups');
}

/**
 * REST GET call for /deployment,
 * stores it on a sessionStorage variable
 */
function getDeployment() {
  return getJSONForTable(REST_V2_PREFIX + '/deployment', 'deployment');
}

function getServerProcessView(table, storageKey) {
  var url = REST_V2_PREFIX + '/servers/view;table=' + table;
  return getJSONForTable(url, storageKey);
}

function getCompactorsView() {
  return getServerProcessView('COMPACTORS', COMPACTOR_SERVER_PROCESS_VIEW);
}

function getCoordinatorQueueView() {
  return getServerProcessView('COORDINATOR_QUEUES', COORDINATOR_QUEUE_PROCESS_VIEW);
}

function getGcView() {
  return getServerProcessView('GC_SUMMARY', GC_SERVER_PROCESS_VIEW);
}

function getGcFileView() {
  return getServerProcessView('GC_FILES', GC_FILE_SERVER_PROCESS_VIEW);
}

function getGcWalView() {
  return getServerProcessView('GC_WALS', GC_WAL_SERVER_PROCESS_VIEW);
}

function getManagersView() {
  return getServerProcessView('MANAGERS', MANAGER_SERVER_PROCESS_VIEW);
}

function getManagersFateView() {
  return getServerProcessView('MANAGER_FATE', MANAGER_FATE_SERVER_PROCESS_VIEW);
}

function getManagersCompactionView() {
  return getServerProcessView('MANAGER_COMPACTIONS', MANAGER_COMPACTION_SERVER_PROCESS_VIEW);
}

function getSserversView() {
  return getServerProcessView('SCAN_SERVERS', SCAN_SERVER_PROCESS_VIEW);
}

function getTserversView() {
  return getServerProcessView('TABLET_SERVERS', TABLET_SERVER_PROCESS_VIEW);
}


/**
 * REST GET call for /tservers/summary,
 * stores it on a sessionStorage variable
 */
function getTserversSummary() {
  return getJSONForTable(REST_V2_PREFIX + '/tservers/summary', 'tserversSummary');
}

/**
 * REST GET call for /instance,
 * stores it on a sessionStorage variable
 */
function getInstanceInfo() {
  return getJSONForTable(REST_V2_PREFIX + '/instance', 'instance');
}

/**
 * REST GET call for /sservers/detail/{group},
 * stores it on a sessionStorage variable
 * @param {string} group Group name
 */
function getSserversDetail(group) {
  const url = `${REST_V2_PREFIX}/sservers/detail/${group}`;
  const sessionDataVar = `sserversDetail_${group}`;
  return getJSONForTable(url, sessionDataVar);
}

/**
 * REST GET call for /compactors/summary,
 * stores it on a sessionStorage variable
 */
function getCompactorsSummary() {
  return getJSONForTable(REST_V2_PREFIX + '/compactors/summary', 'compactorsSummary');
}

/**
 * REST GET call for /tables/{name},
 * stores it on a sessionStorage variable
 * @param {string} name Table name
 */
function getTable(name) {
  const url = `${REST_V2_PREFIX}/tables/${name}`;
  const sessionDataVar = `table_${name}`;
  return getJSONForTable(url, sessionDataVar);
}

/**
 * REST GET call for /compactions/summary,
 * stores it on a sessionStorage variable
 */
function getCompactionsSummary() {
  return getJSONForTable(REST_V2_PREFIX + '/compactions/summary', 'compactionsSummary');
}

/**
 * REST GET call for /compactions/running/table,
 * stores it on a sessionStorage variable
 */
function getRunningCompactionsByTable() {
  return getJSONForTable(REST_V2_PREFIX + '/compactions/running/table', RUNNING_COMPACTIONS_BY_TABLE);
}

/**
 * REST GET call for /compactions/running/group,
 * stores it on a sessionStorage variable
 */
function getRunningCompactionsByGroup() {
  return getJSONForTable(REST_V2_PREFIX + '/compactions/running/group', RUNNING_COMPACTIONS_BY_GROUP);
}


/**
 * Returns true if the input is a valid regular expression, false otherwise.
 *
 * @param {string} input Potential regex string
 * @returns {boolean} Whether the input is a valid regex
 */
function isValidRegex(input) {
  try {
    new RegExp(input);
    return true;
  } catch (e) {
    return false;
  }
}
