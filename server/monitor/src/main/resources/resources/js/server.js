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
    items.push(createEmptyRow(5, 'Empty'));
  } else {
    items.push(createFirstCell(data.details.hostedTablets,
        bigNumberForQuantity(data.details.hostedTablets)));

    items.push(createRightCell(data.details.entries,
        bigNumberForQuantity(data.details.entries)));

    items.push(createRightCell(data.details.minors,
        bigNumberForQuantity(data.details.minors)));

    items.push(createRightCell(data.details.majors,
        bigNumberForQuantity(data.details.majors)));

    items.push(createRightCell(data.details.splits,
        bigNumberForQuantity(data.details.splits)));
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

    row.push(createEmptyRow(8, 'Empty'));

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

      row.push(createFirstCell(val.operation, val.operation));

      row.push(createRightCell(val.success, bigNumberForQuantity(val.success)));

      row.push(createRightCell(val.failure,
          bigNumberForQuantity(val.failure)));

      row.push(createRightCell((val.avgQueueTime == null ?
          '-' : val.avgQueueTime * 1000.0),
          (val.avgQueueTime == null ?
          '&mdash;' : timeDuration(val.avgQueueTime * 1000.0))));

      row.push(createRightCell((val.queueStdDev == null ?
          '-' : val.queueStdDev * 1000.0),
          (val.queueStdDev == null ?
          '&mdash;' : timeDuration(val.queueStdDev * 1000.0))));

      row.push(createRightCell((val.avgTime == null ?
          '-' : val.avgTime * 1000.0),
          (val.avgTime == null ?
          '&mdash;' : timeDuration(val.avgTime * 1000.0))));

      row.push(createRightCell((val.stdDev == null ?
          '-' : val.stdDev * 1000.0),
          (val.stdDev == null ?
          '&mdash;' : timeDuration(val.stdDev * 1000.0))));

      row.push(createRightCell(((val.timeSpent / totalTimeSpent) * 100),
          '<div class="progress"><div class="progress-bar"' +
          ' role="progressbar" style="min-width: 2em; width:' +
          Math.floor((val.timeSpent / totalTimeSpent) * 100) +
          '%;">' + Math.floor((val.timeSpent / totalTimeSpent) * 100) +
          '%</div></div>'));

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
    items.push(createEmptyRow(4, 'Empty'));
  } else {
    var current = data.currentTabletOperationResults;

    items.push(createFirstCell((current.currentMinorAvg == null ?
        '-' : current.currentMinorAvg * 1000.0),
        (current.currentMinorAvg == null ?
        '&mdash;' : timeDuration(current.currentMinorAvg * 1000.0))));

    items.push(createRightCell((current.currentMinorStdDev == null ?
        '-' : current.currentMinorStdDev * 1000.0),
        (current.currentMinorStdDev == null ?
        '&mdash;' : timeDuration(current.currentMinorStdDev * 1000.0))));

    items.push(createRightCell((current.currentMajorAvg == null ?
        '-' : current.currentMajorAvg * 1000.0),
        (current.currentMajorAvg == null ?
        '&mdash;' : timeDuration(current.currentMajorAvg * 1000.0))));

    items.push(createRightCell((current.currentMajorStdDev == null ?
        '-' : current.currentMajorStdDev * 1000.0),
        (current.currentMajorStdDev == null ?
        '&mdash;' : timeDuration(current.currentMajorStdDev * 1000.0))));
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

    row.push(createEmptyRow(11, 'Empty'));

    $('<tr/>', {
      html: row.join('')
    }).appendTo('#perTabletResults');
  } else {
    $.each(data.currentOperations, function(key, val) {
      var row = [];

      row.push(createFirstCell(val.name,
          '<a href="/tables/' + val.tableID + '">' + val.name + '</a>'));

      row.push(createLeftCell(val.tablet, '<code>' + val.tablet + '</code>'));

      row.push(createRightCell((val.entries == null ? 0 : val.entries),
          bigNumberForQuantity(val.entries)));

      row.push(createRightCell((val.ingest == null ? 0 : val.ingest),
          bigNumberForQuantity(Math.floor(val.ingest))));

      row.push(createRightCell((val.query == null ? 0 : val.query),
          bigNumberForQuantity(Math.floor(val.query))));

      row.push(createRightCell((val.minorAvg == null ?
          '-' : val.minorAvg * 1000.0),
          (val.minorAvg == null ?
          '&mdash;' : timeDuration(val.minorAvg * 1000.0))));

      row.push(createRightCell((val.minorStdDev == null ?
          '-' : val.minorStdDev * 1000.0),
          (val.minorStdDev == null ?
          '&mdash;' : timeDuration(val.minorStdDev * 1000.0))));

      row.push(createRightCell((val.minorAvgES == null ? 0 : val.minorAvgES),
          bigNumberForQuantity(Math.floor(val.minorAvgES))));

      row.push(createRightCell((val.majorAvg == null ?
          '-' : val.majorAvg * 1000.0),
          (val.majorAvg == null ?
          '&mdash;' : timeDuration(val.majorAvg * 1000.0))));

      row.push(createRightCell((val.majorStdDev == null ?
          '-' : val.majorStdDev * 1000.0),
          (val.majorStdDev == null ?
          '&mdash;' : timeDuration(val.majorStdDev * 1000.0))));

      row.push(createRightCell((val.majorAvgES == null ?
          0 : val.majorAvgES),
          bigNumberForQuantity(Math.floor(val.majorAvgES))));

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

  caption.push('<span class="table-caption">Details</span><br>');
  caption.push('<span class="table-subcaption">' + server + '</span><br>');

  $('<caption/>', {
    html: caption.join('')
  }).appendTo('#tServerDetail');

  var items = [];

  var columns = ['Hosted&nbsp;Tablets&nbsp;', 'Entries&nbsp;',
      'Minor&nbsp;Compacting&nbsp;', 'Major&nbsp;Compacting&nbsp;',
      'Splitting&nbsp;'];

  for (i = 0; i < columns.length; i++) {
    var first = i == 0 ? true : false;
    items.push(createHeaderCell(first, 'sortTable(0,' + i + ')',
      '', columns[i]));
  }

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
      'Operation&nbsp;Results</span><br>');

  $('<caption/>', {
    html: caption.join('')
  }).appendTo('#opHistoryDetails');

  var items = [];

  var columns = ['Operation&nbsp;', 'Success&nbsp;', 'Failure&nbsp;',
      'Average<br>Queue&nbsp;Time&nbsp;',
      'Std.&nbsp;Dev.<br>Queue&nbsp;Time&nbsp;',
      'Average<br>Time&nbsp;', 'Std.&nbsp;Dev.<br>Time&nbsp;',
      'Percentage&nbsp;Time&nbsp;Spent&nbsp;'];

  for (i = 0; i < columns.length; i++) {
    var first = i == 0 ? true : false;
    items.push(createHeaderCell(first, 'sortTable(1,' + i + ')',
      '', columns[i]));
  }

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
      'Operation&nbsp;Results</span><br>');

  $('<caption/>', {
    html: caption.join('')
  }).appendTo('#currentTabletOps');

  var items = [];

  var columns = ['Minor&nbsp;Average&nbsp;', 'Minor&nbsp;Std&nbsp;Dev&nbsp;',
      'Major&nbsp;Avg&nbsp;', 'Major&nbsp;Std&nbsp;Dev&nbsp;'];

  for (i = 0; i < columns.length; i++) {
    var first = i == 0 ? true : false;
    items.push(createHeaderCell(first, 'sortTable(2,' + i + ')',
      '', columns[i]));
  }

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
      'Operations</span><br>');
  caption.push('<span class="table-subcaption">Per-tablet&nbsp;' +
      'Details</span><br>');

  $('<caption/>', {
    html: caption.join('')
  }).appendTo('#perTabletResults');

  var items = [];

  var columns = ['Table&nbsp;', 'Tablet&nbsp;', 'Entries&nbsp;',
      'Ingest&nbsp;', 'Query&nbsp;', 'Minor&nbsp;Avg&nbsp;',
      'Minor&nbsp;Std&nbsp;Dev&nbsp;', 'Minor&nbsp;Avg&nbsp;e/s&nbsp;',
      'Major&nbsp;Avg&nbsp;', 'Major&nbsp;Std&nbsp;Dev&nbsp;',
      'Major&nbsp;Avg&nbsp;e/s&nbsp;'];

  for (i = 0; i < columns.length; i++) {
    var first = i == 0 ? true : false;
    items.push(createHeaderCell(first, 'sortTable(3,' + i + ')',
      '', columns[i]));
  }

  $('<tr/>', {
    html: items.join('')
  }).appendTo('#perTabletResults');
}
