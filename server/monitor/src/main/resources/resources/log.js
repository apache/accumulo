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

/**
 * Creates bulk import initial table
 */
$(document).ready(function() {
  createHeader();
  refreshLogs();
});

/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshLogs() {
  $.ajaxSetup({
    async: false
  });
  getLogs();
  $.ajaxSetup({
    async: true
  });
  createHeader();
  createLogsTable();
}

/**
 * Used to set the refresh interval to 5 seconds
 */
function refresh() {
  clearInterval(TIMER);
  if (sessionStorage.autoRefresh == 'true') {
    TIMER = setInterval('refreshLogs()', 5000);
  }
}

/**
 * Clears the log table
 */
function clearLogTable() {
  clearLogs();
  refreshLogs();
  refreshNavBar();
}

/**
 * Generates the log table
 */
function createLogsTable() {
  clearTable('logTable');

  /*
   * Get the logs value obtained earlier,
   * if it doesn't exists, create an empty array
   */
  var data = sessionStorage.logs === undefined ?
      [] : JSON.parse(sessionStorage.logs);

  /*
   * If the data is empty, create an empty row, otherwise,
   * create the rows for the table
   */
  if (data.length === 0) {
    var items = [];
    items.push('<td class="center" colspan="5"><i>Empty</i></td>');
    $('<tr/>', {
      html: items.join('')
    }).appendTo('#logTable');
  } else {
    $.each(data, function(key, val) {
      var items = [];
      var date = new Date(val.timestamp);
      items.push('<td class="firstcell left" data-value="' + val.timestamp +
          '">' + date.toLocaleString().split(' ').join('&nbsp;') + '</td>');

      items.push('<td class="center" data-value="' + val.application + '">' +
          val.application + '</td>');

      items.push('<td class="right" data-value="' + val.count + '">' +
          bigNumberForQuantity(val.count) + '</td>');
      items.push('<td class="center" data-value="' + val.level + '">' +
          levelFormat(val.level) + '</td>');
      items.push('<td class="center" data-value="' + val.message + '">' +
          val.message + '</td>');

      $('<tr/>', {
        html: items.join('')
      }).appendTo('#logTable');
    });
  }
}

/**
 * Formats the log level as HTML
 *
 * @param {string} level Log level
 * @return {string} HTML formatted level
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
 * Sorts the logTable table on the selected column
 *
 * @param {number} n Column number to sort by
 */
function sortTable(n) {
  if (sessionStorage.tableColumnSort !== undefined &&
      sessionStorage.tableColumnSort == n &&
      sessionStorage.direction !== undefined) {
    direction = sessionStorage.direction === 'asc' ? 'desc' : 'asc';
  } else {
    direction = sessionStorage.direction === undefined ?
        'asc' : sessionStorage.direction;
  }
  sessionStorage.tableColumnSort = n;
  sortTables('logTable', direction, n);
}

/**
 * Creates the log table header
 */
function createHeader() {
  $('#logTable caption').remove();
  var caption = [];

  caption.push('<span class="table-caption">Recent Logs</span><br />');

  var data = sessionStorage.logs === undefined ?
      [] : JSON.parse(sessionStorage.logs);
  if (data.length !== 0) {
    caption.push('<a href="javascript:clearLogTable();">Clear&nbsp;' +
        'All&nbsp;Events</a>');
  }

  $('<caption/>', {
    html: caption.join('')
  }).appendTo('#logTable');

  var items = [];

  /*
   * Adds the columns, add sortTable function on click,
   * if the column has a description, add title taken from the global.js
   */
  items.push('<th class="firstcell" onclick="sortTable(0)">Time&nbsp;</th>');
  items.push('<th onclick="sortTable(1)">Application&nbsp;</th>');
  items.push('<th onclick="sortTable(2)">Count&nbsp;</th>');
  items.push('<th onclick="sortTable(3)">Level&nbsp;</th>');
  items.push('<th onclick="sortTable(4)">Message&nbsp;</th>');

  $('<tr/>', {
    html: items.join('')
  }).appendTo('#logTable');
}
