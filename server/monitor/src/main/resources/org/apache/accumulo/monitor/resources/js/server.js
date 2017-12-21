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
var tabletResults;
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
  tabletResults.ajax.reload();
}