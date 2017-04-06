/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

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
  $('.auto-refresh').click(function(e) {
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
  // If the number is smaller than the base, return thee number with no suffix
  if (big < base) {
    return big + suffixes[0];
  }
  // Finds which suffix to use
  var exp = Math.floor(Math.log(big) / Math.log(base));
  // Divides the bumber by the equivalent suffix number
  var val = big / Math.pow(base, exp);
  // Keeps the number to 2 decimal places and adds the suffix
  return val.toFixed(2) + suffixes[exp];
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
 * Sorts the selected table by column in the direction chosen
 *
 * @param {string} tableID Table to sort
 * @param {string} direction Direction to sort table, asc or desc
 * @param {number} n Column to sort
 */
function sortTables(tableID, direction, n) {
  var table, rows, switching, i, x, y, h, shouldSwitch, dir, xFinal, yFinal;
  table = document.getElementById(tableID);
  switching = true;

  dir = direction;
  sessionStorage.direction = dir;

  // Select the rows of the table
  rows = table.getElementsByTagName('tr');

  // Clears the sortable class from the table columns
  var count = 0;
  while (rows[0].getElementsByTagName('th').length > count) {
    var tmpH = rows[0].getElementsByTagName('th')[count];
    tmpH.classList.remove('sortable');
    if (rows.length > 2) {
      tmpH.classList.add('sortable');
    }
    $(tmpH.getElementsByTagName('span')).remove();
    count += 1;
  }

  // If there are more than 2 rows, add arrow to the selected column
  if (rows.length <= 2) {
      switching = false;
  } else {
    h = rows[0].getElementsByTagName('th')[n];
    if (dir == 'asc') {
      $(h).append('<span class="glyphicon glyphicon-chevron-up"' +
          ' width="10px" height="10px" />');
    } else if (dir == 'desc') {
      $(h).append('<span class="glyphicon glyphicon-chevron-down"' +
          ' width="10px" height="10px" />');
    }
  }

  /*
   * Make a loop that will continue until
   * no switching has been done:
   */
  while (switching) {
    switching = false;
    rows = table.getElementsByTagName('tr');

    /*
     * Loop through all table rows (except the
     * first, which contains table headers):
     */
    for (i = 1; i < (rows.length - 1); i++) {
      shouldSwitch = false;
      /*
       * Get two elements to compare,
       * one from current row and one from the next:
       * If the element is a dash, convert to null, otherwise,
       * if it is a string, convert to number
       */
      x = rows[i].getElementsByTagName('td')[n].getAttribute('data-value');
      xFinal = (x === '-' || x === '&mdash;' ?
          null : (Number(x) == x ? Number(x) : x));

      y = rows[i + 1].getElementsByTagName('td')[n].getAttribute('data-value');
      yFinal = (y === '-' || y === '&mdash;' ?
          null : (Number(y) == y ? Number(y) : y));

      /*
       * Check if the two rows should switch place,
       * based on the direction, asc or desc:
       */
      if (dir == 'asc') {
        if (xFinal > yFinal || (xFinal !== null && yFinal === null)) {
          // if so, mark as a switch and break the loop:
          shouldSwitch = true;
          break;
        }
      } else if (dir == 'desc') {
        if (xFinal < yFinal || (yFinal !== null && xFinal === null)) {
          // if so, mark as a switch and break the loop:
          shouldSwitch = true;
          break;
        }
      }
    }
    if (shouldSwitch) {
      /*
       * If a switch has been marked, make the switch
       * and mark that a switch has been done:
       */
      rows[i].parentNode.insertBefore(rows[i + 1], rows[i]);
      switching = true;
    }
  }
}

/**
 * Clears the selected table while leaving the headers
 *
 * @param {string} tableID Table to clear
 */
function clearTable(tableID) {
  // JQuery selector to select all rows except for the first row (header)
  $('#' + tableID).find('tr:not(:first)').remove();
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
 * Creates a cell for the header
 *
 * @param {boolean} firstCell Defines a first cell
 * @param {string} onClick Function to select on click
 * @param {string} title Title for tooltip
 * @param {string} showValue Value for the cell
 */
function createHeaderCell(firstCell, onClick, title, showValue) {
  var cellClass = firstCell ? ' class="firstcell"' : '';
  var clickFunc = onClick != '' ? (' onclick="' + onClick + '"') : '';
  var cellTitle = title != '' ? (' title="' + title + '"') : '';

  return '<th' + cellClass + clickFunc + cellTitle + '>' + showValue + '</th>';
}

/**
 * Creates a plot on the selected id, with the data
 * The type of the plot depends on the type:
 * type = 0 -> Single lines plot
 * type = 1 -> Single points plot
 * type = 2 -> Double lines plot
 *
 * @param {string} id Canvas ID
 * @param {object|array} inData Data to plot
 * @param {number} type Type of plot
 */
function makePlot(id, inData, type) {
  var d = new Date();
  var n = d.getTimezoneOffset() * 60000; // Converts offset to milliseconds
  var tz = new Date().toLocaleTimeString('en-us',
      {timeZoneName: 'short'}).split(' ')[2]; // Short version of timezone
  var tzFormat = '%H:%M<br>' + tz;

  var dataInfo = [];

  // Select the type of plot
  switch (type) {
    // Single lines plot
    case 0:
      dataInfo.push({ data: inData,
          lines: { show: true },
          color: '#d9534f' });
      break;
    // Single points plot
    case 1:
      dataInfo.push({ data: inData,
          points: { show: true, radius: 1 },
          color: '#d9534f' });
      break;
    // Double lines plot
    case 2:
      dataInfo.push({ label: 'Read',
          data: inData.Read,
          lines: { show: true },
          color: '#d9534f' })
      dataInfo.push({ label: 'Returned',
          data: inData.Returned,
          lines: { show: true },
          color: '#337ab7' });
      break;
    default:
      dataInfo = [];
  }

  // Format the plot axis
  var plotInfo = {yaxis: {}, xaxis: {mode: 'time', minTickSize: [1, 'minute'],
  timeFormat: tzFormat, ticks: 3}};

  // Plot the data
  $.plot($('#' + id), dataInfo, plotInfo);
}

///// REST Calls /////////////

/**
 * REST GET call for the master information,
 * stores it on a sessionStorage variable
 */
function getMaster() {
  $.getJSON('/rest/master', function(data) {
    sessionStorage.master = JSON.stringify(data);
  });
}

/**
 * REST GET call for the zookeeper information,
 * stores it on a sessionStorage variable
 */
function getZK() {
  $.getJSON('/rest/zk', function(data) {
    sessionStorage.zk = JSON.stringify(data);
  });
}

/**
 * REST GET call for the namespaces, stores it on a global variable
 */
function getNamespaces() {
  $.getJSON('/rest/tables/namespaces', function(data) {
    NAMESPACES = JSON.stringify(data);
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
    getTables();
  } else {
    // Convert the list to a string for the REST call
    namespaceList = namespaces.toString();

    var call = '/rest/tables/namespaces/' + namespaceList;
    $.getJSON(call, function(data) {
      sessionStorage.tables = JSON.stringify(data);
    });
  }
}

/**
 * REST GET call for the tables, stores it on a sessionStorage variable
 */
function getTables() {
  $.getJSON('/rest/tables', function(data) {
    sessionStorage.tables = JSON.stringify(data);
  });
}

/**
 * REST POST call to clear a specific dead server
 *
 * @param {string} server Dead Server ID
 */
function clearDeadServers(server) {
  var call = '/rest/tservers?server=' + server;
  $.post(call);
}

/**
 * REST GET call for the tservers, stores it on a sessionStorage variable
 */
function getTServers() {
  $.getJSON('/rest/tservers', function(data) {
    sessionStorage.tservers = JSON.stringify(data);
  });
}

/**
 * REST GET call for the tservers, stores it on a sessionStorage variable
 *
 * @param {string} server Server ID
 */
function getTServer(server) {
  var call = '/rest/tservers/' + server;
  $.getJSON(call, function(data) {
    sessionStorage.server = JSON.stringify(data);
  });
}

/**
 * REST GET call for the scans, stores it on a sessionStorage variable
 */
function getScans() {
  $.getJSON('/rest/scans', function(data) {
    sessionStorage.scans = JSON.stringify(data);
  });
}

/**
 * REST GET call for the bulk imports, stores it on a sessionStorage variable
 */
function getBulkImports() {
  $.getJSON('/rest/bulkImports', function(data) {
    sessionStorage.bulkImports = JSON.stringify(data);
  });
}

/**
 * REST GET call for the garbage collector,
 * stores it on a sessionStorage variable
 */
function getGarbageCollector() {
  $.getJSON('/rest/gc', function(data) {
    sessionStorage.gc = JSON.stringify(data);
  });
}

/**
 * REST GET call for the server stats, stores it on a sessionStorage variable
 */
function getServerStats() {
  $.getJSON('/rest/tservers/serverStats', function(data) {
    sessionStorage.serverStats = JSON.stringify(data);
  });
}

/**
 * REST GET call for the recovery list, stores it on a sessionStorage variable
 */
function getRecoveryList() {
  $.getJSON('/rest/tservers/recovery', function(data) {
    sessionStorage.recoveryList = JSON.stringify(data);
  });
}

/**
 * REST GET call for the participating tablet servers,
 * stores it on a sessionStorage variable
 *
 * @param {string} table Table ID
 */
function getTableServers(table) {
  var call = '/rest/tables/' + table;
  $.getJSON(call, function(data) {
    sessionStorage.tableServers = JSON.stringify(data);
  });
}

/**
 * REST GET call for the trace summary, stores it on a sessionStorage variable
 *
 * @param {string} minutes Number of minutes to display trace summary
 */
function getTraceSummary(minutes) {
  var call = '/rest/trace/summary/' + minutes;
  $.getJSON(call, function(data) {
    sessionStorage.traceSummary = JSON.stringify(data);
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
  $.getJSON(call, function(data) {
    sessionStorage.traceType = JSON.stringify(data);
  });
}

/**
 * REST GET call for the trace id, stores it on a sessionStorage variable
 *
 * @param {string} id Trace ID
 */
function getTraceShow(id) {
  var call = '/rest/trace/show/' + id;
  $.getJSON(call, function(data) {
    sessionStorage.traceShow = JSON.stringify(data);
  });
}

/**
 * REST GET call for the logs, stores it on a sessionStorage variable
 */
function getLogs() {
  $.getJSON('/rest/logs', function(data) {
    sessionStorage.logs = JSON.stringify(data);
  });
}

/**
 * REST POST call to clear logs
 */
function clearLogs() {
  $.post('/rest/logs');
}

/**
 * REST GET call for the problems
 */
function getProblems() {
  getProblemSummary();
  getProblemDetails();
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
  $.post(call);
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
  $.post(call);
}

/**
 * REST GET call for the problems summary,
 * stores it on a sessionStorage variable
 */
function getProblemSummary() {
  $.getJSON('/rest/problems/summary', function(data) {
    sessionStorage.problemSummary = JSON.stringify(data);
  });
}

/**
 * REST GET call for the problems details,
 * stores it on a sessionStorage variable
 */
function getProblemDetails() {
  $.getJSON('/rest/problems/details', function(data) {
    sessionStorage.problemDetails = JSON.stringify(data);
  });
}

/**
 * REST GET call for the replication table,
 * stores it on a sessionStorage variable
 */
function getReplication() {
  $.getJSON('/rest/replication', function(data) {
    sessionStorage.replication = JSON.stringify(data);
  });
}

/**
 * Creates a banner
 *
 * @param {string} id Banner ID
 * @param {string} bannerClass Class for the banner
 * @param {string} text Text to display on the banner
 */
function doBanner(id, bannerClass, text) {
  $('<div/>', {
   html: text,
   class: 'alert alert-' + bannerClass,
   role: 'alert'
  }).appendTo('#' + id);
}

//// Overview Plots Rest Calls

/**
 * REST GET call for the ingest rate,
 * stores it on a sessionStorage variable
 */
function getIngestRate() {
  $.getJSON('/rest/statistics/time/ingestRate', function(data) {
    sessionStorage.ingestRate = JSON.stringify(data);
  });
}

/**
 * REST GET call for the scan entries,
 * stores it on a sessionStorage variable
 */
function getScanEntries() {
  $.getJSON('/rest/statistics/time/scanEntries', function(data) {
    sessionStorage.scanEntries = JSON.stringify(data);
  });
}

/**
 * REST GET call for the ingest byte rate,
 * stores it on a sessionStorage variable
 */
function getIngestByteRate() {
  $.getJSON('/rest/statistics/time/ingestByteRate', function(data) {
    sessionStorage.ingestMB = JSON.stringify(data);
  });
}

/**
 * REST GET call for the query byte rate, stores it on a sessionStorage variable
 */
function getQueryByteRate() {
  $.getJSON('/rest/statistics/time/queryByteRate', function(data) {
    sessionStorage.queryMB = JSON.stringify(data);
  });
}

/**
 * REST GET call for the load average, stores it on a sessionStorage variable
 */
function getLoadAverage() {
  $.getJSON('/rest/statistics/time/load', function(data) {
    sessionStorage.loadAvg = JSON.stringify(data);
  });
}

/**
 * REST GET call for the lookups, stores it on a sessionStorage variable
 */
function getLookups() {
  $.getJSON('/rest/statistics/time/lookups', function(data) {
    sessionStorage.lookups = JSON.stringify(data);
  });
}

/**
 * REST GET call for the minor compactions,
 * stores it on a sessionStorage variable
 */
function getMinorCompactions() {
  $.getJSON('/rest/statistics/time/minorCompactions', function(data) {
    sessionStorage.minorCompactions = JSON.stringify(data);
  });
}

/**
 * REST GET call for the major compactions,
 * stores it on a sessionStorage variable
 */
function getMajorCompactions() {
  $.getJSON('/rest/statistics/time/majorCompactions', function(data) {
    sessionStorage.majorCompactions = JSON.stringify(data);
  });
}

/**
 * REST GET call for the index cache hit rate,
 * stores it on a sessionStorage variable
 */
function getIndexCacheHitRate() {
  $.getJSON('/rest/statistics/time/indexCacheHitRate', function(data) {
    sessionStorage.indexCache = JSON.stringify(data);
  });
}

/**
 * REST GET call for the data cache hit rate,
 * stores it on a sessionStorage variable
 */
function getDataCacheHitRate() {
  $.getJSON('/rest/statistics/time/dataCacheHitRate', function(data) {
    sessionStorage.dataCache = JSON.stringify(data);
  });
}

/**
 * REST GET call for the server status, stores it on a sessionStorage variable
 */
function getStatus() {
  $.getJSON('/rest/status', function(data) {
    sessionStorage.status = JSON.stringify(data);
  });
}
