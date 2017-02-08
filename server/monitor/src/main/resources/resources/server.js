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

var serv;
/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshServer() {
  $.ajaxSetup({
    async: false
  });
  getTServer(serv);
  $.ajaxSetup({
    async: true
  });
  refreshDetailTable();
  refreshHistoryTable();
  refreshCurrentTable();
  refreshResultsTable();
}

/**
 * Used to redraw the page
 */
function refresh() {
  refreshServer();
}

/**
 * Generates the server details table
 */
function refreshDetailTable() {

  $('#tServerDetail tr:gt(0)').remove();

  var data = sessionStorage.server === undefined ?
      [] : JSON.parse(sessionStorage.server);

  var items = [];

  if (data.length === 0 || data.details === undefined) {
    items.push('<td class="center" colspan="5"><i>Empty</i></td>');
  } else {
    items.push('<td class="firstcell right" data-value="' +
        data.details.hostedTablets + '">' +
        bigNumberForQuantity(data.details.hostedTablets) + '</td>');

    items.push('<td class="right" data-value="' +
        data.details.entries + '">' +
        bigNumberForQuantity(data.details.entries) + '</td>');

    items.push('<td class="right" data-value="' +
        data.details.minors + '">' +
        bigNumberForQuantity(data.details.minors) + '</td>');

    items.push('<td class="right" data-value="' +
        data.details.majors + '">' +
        bigNumberForQuantity(data.details.majors) + '</td>');

    items.push('<td class="right" data-value="' +
        data.details.splits + '">' +
        bigNumberForQuantity(data.details.splits) + '</td>');
  }

  $('<tr/>', {
    html: items.join('')
  }).appendTo('#tServerDetail');
}

/**
 * Generates the server history table
 */
function refreshHistoryTable() {

  $('#opHistoryDetails tr:gt(0)').remove();

  var data = sessionStorage.server === undefined ?
      [] : JSON.parse(sessionStorage.server);

  if (data.length === 0 || data.allTimeTabletResults === undefined) {
    var row = [];

    row.push('<td class="center" colspan="8"><i>Empty</i></td>');

    $('<tr/>', {
      html: row.join('')
    }).appendTo('#opHistoryDetails');
  } else {
    var totalTimeSpent = 0;
    $.each(data.allTimeTabletResults, function(key, val) {
      totalTimeSpent += val.timeSpent;
    });

    var count = 0;
    $.each(data.allTimeTabletResults, function(key, val) {
      var row = [];

      row.push('<td class="firstcell left" data-value="' + val.operation +
          '">' + val.operation + '</td>');

      row.push('<td class="right" data-value="' + val.success +
          '">' + bigNumberForQuantity(val.success) + '</td>');

      row.push('<td class="right" data-value="' + val.failure +
          '">' + bigNumberForQuantity(val.failure) + '</td>');

      row.push('<td class="right" data-value="' +
          (val.avgQueueTime == null ? '-' : val.avgQueueTime * 1000.0) +
          '">' + (val.avgQueueTime == null ?
          '&mdash;' : timeDuration(val.avgQueueTime * 1000.0)) + '</td>');

      row.push('<td class="right" data-value="' +
          (val.queueStdDev == null ? '-' : val.queueStdDev * 1000.0) +
          '">' + (val.queueStdDev == null ?
          '&mdash;' : timeDuration(val.queueStdDev * 1000.0)) + '</td>');

      row.push('<td class="right" data-value="' +
          (val.avgTime == null ? '-' : val.avgTime * 1000.0) +
          '">' + (val.avgTime == null ?
          '&mdash;' : timeDuration(val.avgTime * 1000.0)) + '</td>');

      row.push('<td class="right" data-value="' +
          (val.stdDev == null ? '-' : val.stdDev * 1000.0) +
          '">' + (val.stdDev == null ?
          '&mdash;' : timeDuration(val.stdDev * 1000.0)) + '</td>');

      row.push('<td class="right" data-value="' +
          ((val.timeSpent / totalTimeSpent) * 100) +
          '"><div class="progress"><div class="progress-bar"' +
          ' role="progressbar" style="min-width: 2em; width:' +
          Math.floor((val.timeSpent / totalTimeSpent) * 100) +
          '%;">' + Math.floor((val.timeSpent / totalTimeSpent) * 100) + '%</div></div></td>');

      $('<tr/>', {
        html: row.join('')
      }).appendTo('#opHistoryDetails');

    });
  }
}

/**
 * Generates the current server table
 */
function refreshCurrentTable() {

  $('#currentTabletOps tr:gt(0)').remove();

  var data = sessionStorage.server === undefined ?
      [] : JSON.parse(sessionStorage.server);

  var items = [];
  if (data.length === 0 || data.currentTabletOperationResults === undefined) {
    items.push('<td class="center" colspan="4"><i>Empty</i></td>');
  } else {
    var current = data.currentTabletOperationResults;

    items.push('<td class="firstcell right" data-value="' +
        (current.currentMinorAvg == null ?
        '-' : current.currentMinorAvg * 1000.0) + '">' +
        (current.currentMinorAvg == null ?
        '&mdash;' : timeDuration(current.currentMinorAvg * 1000.0)) +
        '</td>');

    items.push('<td class="right" data-value="' +
        (current.currentMinorStdDev == null ?
        '-' : current.currentMinorStdDev * 1000.0) + '">' +
        (current.currentMinorStdDev == null ?
        '&mdash;' : timeDuration(current.currentMinorStdDev * 1000.0)) +
        '</td>');

    items.push('<td class="right" data-value="' +
        (current.currentMajorAvg == null ?
        '-' : current.currentMajorAvg * 1000.0) + '">' +
        (current.currentMajorAvg == null ?
        '&mdash;' : timeDuration(current.currentMajorAvg * 1000.0)) +
        '</td>');

    items.push('<td class="right" data-value="' +
        (current.currentMajorStdDev == null ?
        '-' : current.currentMajorStdDev * 1000.0) + '">' +
        (current.currentMajorStdDev == null ?
        '&mdash;' : timeDuration(current.currentMajorStdDev * 1000.0)) +
        '</td>');
  }

  $('<tr/>', {
      html: items.join('')
  }).appendTo('#currentTabletOps');

}

/**
 * Generates the server results table
 */
function refreshResultsTable() {

  $('#perTabletResults tr:gt(0)').remove();

  var data = sessionStorage.server === undefined ?
      [] : JSON.parse(sessionStorage.server);

  if (data.length === 0 || data.currentOperations === undefined) {
    var row = [];

    row.push('<td class="center" colspan="11"><i>Empty</i></td>');

    $('<tr/>', {
      html: row.join('')
    }).appendTo('#perTabletResults');
  } else {
    $.each(data.currentOperations, function(key, val) {
      var row = [];

      row.push('<td class="firstcell left" data-value="' + val.name +
          '"><a href="/tables/' + val.tableID + '">' + val.name + '</a></td>');

      row.push('<td class="left" data-value="' + val.tablet + '"><code>' +
          val.tablet + '</code></td>');

      row.push('<td class="right" data-value="' +
          (val.entries == null ? 0 : val.entries) + '">' +
          bigNumberForQuantity(val.entries) + '</td>');

      row.push('<td class="right" data-value="' +
          (val.ingest == null ? 0 : val.ingest) + '">' +
          bigNumberForQuantity(Math.floor(val.ingest)) + '</td>');

      row.push('<td class="right" data-value="' +
          (val.query == null ? 0 : val.query) + '">' +
          bigNumberForQuantity(Math.floor(val.query)) + '</td>');

      row.push('<td class="right" data-value="' +
          (val.minorAvg == null ? '-' : val.minorAvg * 1000.0) + '">' +
          (val.minorAvg == null ?
          '&mdash;' : timeDuration(val.minorAvg * 1000.0)) + '</td>');

      row.push('<td class="right" data-value="' +
          (val.minorStdDev == null ? '-' : val.minorStdDev * 1000.0) + '">' +
          (val.minorStdDev == null ?
          '&mdash;' : timeDuration(val.minorStdDev * 1000.0)) + '</td>');

      row.push('<td class="right" data-value="' +
          (val.minorAvgES == null ? 0 : val.minorAvgES) + '">' +
          bigNumberForQuantity(Math.floor(val.minorAvgES)) + '</td>');

      row.push('<td class="right" data-value="' +
          (val.majorAvg == null ? '-' : val.majorAvg * 1000.0) + '">' +
          (val.majorAvg == null ?
          '&mdash;' : timeDuration(val.majorAvg * 1000.0)) + '</td>');

      row.push('<td class="right" data-value="' +
          (val.majorStdDev == null ? '-' : val.majorStdDev * 1000.0) + '">' +
          (val.majorStdDev == null ?
          '&mdash;' : timeDuration(val.majorStdDev * 1000.0)) + '</td>');

      row.push('<td class="right" data-value="' +
          (val.majorAvgES == null ? 0 : val.majorAvgES) + '">' +
          bigNumberForQuantity(Math.floor(val.majorAvgES)) + '</td>');

      $('<tr/>', {
        html: row.join('')
      }).appendTo('#perTabletResults');
    });
  }
}

/**
 * Sorts the different server status tables on the selected column
 *
 * @param {string} table Table ID to sort
 * @param {number} n Column number to sort by
 */
function sortTable(table, n) {
  var tableIDs = ['tServerDetail', 'opHistoryDetails',
      'currentTabletOps', 'perTabletResults'];

  if (sessionStorage.tableColumnSort !== undefined &&
      sessionStorage.tableColumnSort == n &&
      sessionStorage.direction !== undefined) {
    direction = sessionStorage.direction === 'asc' ? 'desc' : 'asc';
  } else {
    direction = sessionStorage.direction === undefined ?
        'asc' : sessionStorage.direction;
  }
  sessionStorage.tableColumn = tableIDs[table];
  sessionStorage.tableColumnSort = n;

  sortTables(tableIDs[table], direction, n);
}

/**
 * Creates the server detail header
 *
 * @param {string} server Server name
 */
function createDetailHeader(server) {
  var caption = [];
  serv = server;

  caption.push('<span class="table-caption">Details</span><br />');
  caption.push('<span class="table-subcaption">' + server + '</span><br />');

  $('<caption/>', {
    html: caption.join('')
  }).appendTo('#tServerDetail');

  var items = [];

  items.push('<th class="firstcell" onclick="sortTable(0,0)">Hosted&nbsp;' +
      'Tablets&nbsp;</th>');
  items.push('<th onclick="sortTable(0,1)">Entries&nbsp;</th>');
  items.push('<th onclick="sortTable(0,2)">Minor&nbsp;Compacting&nbsp;</th>');
  items.push('<th onclick="sortTable(0,3)">Major&nbsp;Compacting&nbsp;</th>');
  items.push('<th onclick="sortTable(0,4)">Splitting&nbsp;</th>');

  $('<tr/>', {
    html: items.join('')
  }).appendTo('#tServerDetail');
}

/**
 * Creates the server history header
 */
function createHistoryHeader() {
  var caption = [];

  caption.push('<span class="table-caption">All-Time&nbsp;Tablet&nbsp;' +
      'Operation&nbsp;Results</span><br />');

  $('<caption/>', {
    html: caption.join('')
  }).appendTo('#opHistoryDetails');

  var items = [];

  items.push('<th class="firstcell" onclick="sortTable(1,0)">' +
      'Operation&nbsp;</th>');
  items.push('<th onclick="sortTable(1,1)">Success&nbsp;</th>');
  items.push('<th onclick="sortTable(1,2)">Failure&nbsp;</th>');
  items.push('<th onclick="sortTable(1,3)">Average<br />Queue&nbsp;' +
      'Time&nbsp;</th>');
  items.push('<th onclick="sortTable(1,4)">Std.&nbsp;Dev.<br />Queue&nbsp;' +
      'Time&nbsp;</th>');
  items.push('<th onclick="sortTable(1,5)">Average<br />Time&nbsp;</th>');
  items.push('<th onclick="sortTable(1,6)">Std.&nbsp;Dev.<br />Time' +
      '&nbsp;</th>');
  items.push('<th onclick="sortTable(1,7)">Percentage&nbsp;Time&nbsp;' +
      'Spent&nbsp;');

  $('<tr/>', {
    html: items.join('')
  }).appendTo('#opHistoryDetails');
}

/**
 * Creates the current server header
 */
function createCurrentHeader() {
  var caption = [];

  caption.push('<span class="table-caption">Current&nbsp;Tablet&nbsp;' +
      'Operation&nbsp;Results</span><br />');

  $('<caption/>', {
    html: caption.join('')
  }).appendTo('#currentTabletOps');

  var items = [];

  items.push('<th class="firstcell" onclick="sortTable(2,0)">Minor&nbsp;' +
      'Average&nbsp;</th>');
  items.push('<th onclick="sortTable(2,1)">Minor&nbsp;Std&nbsp;Dev&nbsp;</th>');
  items.push('<th onclick="sortTable(2,2)">Major&nbsp;Avg&nbsp;</th>');
  items.push('<th onclick="sortTable(2,3)">Major&nbsp;Std&nbsp;Dev&nbsp;</th>');

  $('<tr/>', {
    html: items.join('')
  }).appendTo('#currentTabletOps');
}

/**
 * Creates the server result header
 */
function createResultsHeader() {
  var caption = [];

  caption.push('<span class="table-caption">Detailed&nbsp;Current&nbsp;' +
      'Operations</span><br />');
  caption.push('<span class="table-subcaption">Per-tablet&nbsp;' +
      'Details</span><br />');

  $('<caption/>', {
    html: caption.join('')
  }).appendTo('#perTabletResults');

  var items = [];

  items.push('<th class="firstcell" onclick="sortTable(3,0)">Table&nbsp;</th>');
  items.push('<th onclick="sortTable(3,1)">Tablet&nbsp;</th>');
  items.push('<th onclick="sortTable(3,2)">Entries&nbsp;</th>');
  items.push('<th onclick="sortTable(3,3)">Ingest&nbsp;</th>');
  items.push('<th onclick="sortTable(3,4)">Query&nbsp;</th>');
  items.push('<th onclick="sortTable(3,5)">Minor&nbsp;Avg&nbsp;</th>');
  items.push('<th onclick="sortTable(3,6)">Minor&nbsp;Std&nbsp;Dev&nbsp;</th>');
  items.push('<th onclick="sortTable(3,7)">Minor&nbsp;Avg&nbsp;e/s&nbsp;</th>');
  items.push('<th onclick="sortTable(3,8)">Major&nbsp;Avg&nbsp;</th>');
  items.push('<th onclick="sortTable(3,9)">Major&nbsp;Std&nbsp;Dev&nbsp;</th>');
  items.push('<th onclick="sortTable(3,10)">Major&nbsp;' +
      'Avg&nbsp;e/s&nbsp;</th>');

  $('<tr/>', {
    html: items.join('')
  }).appendTo('#perTabletResults');
}
