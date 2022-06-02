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

var serv;
var tabletResults;
/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshServer() {
  getTServer(serv).then(function () {
    refreshDetailTable();
    refreshHistoryTable();
    refreshCurrentTable();
    refreshResultsTable();
  });
}

/**
 * Used to redraw the page
 */
function refresh() {
  refreshServer();
}

/**
 * Populates the server details table
 */
function refreshDetailTable() {
  var data = sessionStorage.server === undefined ? [] : JSON.parse(sessionStorage.server);
  if (data.length === 0 || data.details === undefined) {
    clearAllTableCells("tServerDetail");
  } else {
    $("#hostedTablets").text(bigNumberForQuantity(data.details.hostedTablets));
    $("#entries").text(bigNumberForQuantity(data.details.entries));
    $("#minors").text(bigNumberForQuantity(data.details.minors));
    $("#majors").text(bigNumberForQuantity(data.details.majors));
    $("#splits").text(bigNumberForQuantity(data.details.splits));
  }
}

/**
 * Populates the All Time Tablet Operations table
 */
function refreshHistoryTable() {
  var data = sessionStorage.server === undefined ? [] : JSON.parse(sessionStorage.server);

  if (data.length === 0 || data.allTimeTabletResults === undefined) {
    clearAllTableCells("opHistoryDetails");
  } else {
    var totalTimeSpent = 0;
    $.each(data.allTimeTabletResults, function (key, val) {
      totalTimeSpent += val.timeSpent;
    });

    $.each(data.allTimeTabletResults, function (key, val) {
      // use the first 5 characters of the operation for the jquery selector
      var rowId = "#" + val.operation.slice(0, 5) + "Row";
      console.log("Populating rowId " + rowId + " with " + val.operation + " data");

      $(rowId).append("<td>" + val.operation + "</td>");
      $(rowId).append("<td>" + bigNumberForQuantity(val.success) + "</td>");
      $(rowId).append("<td>" + bigNumberForQuantity(val.failure) + "</td>");

      appendDurationToRow(rowId, val.avgQueueTime);
      appendDurationToRow(rowId, val.queueStdDev);
      appendDurationToRow(rowId, val.avgTime);
      appendDurationToRow(rowId, val.stdDev);

      var percent = Math.floor((val.timeSpent / totalTimeSpent) * 100);
      var progressBarCell = '<td><div class="progress"><div class="progress-bar"' +
        ' role="progressbar" style="min-width: 2em; width:' + percent + '%;">' +
        percent + '%</div></div></td>';
      console.log("Time spent percent = " + val.timeSpent + "/" + totalTimeSpent + " " + percent);

      $(rowId).append(progressBarCell);
    });
  }
}

/**
 * Populates the current tablet operations results table
 */
function refreshCurrentTable() {
  var data = sessionStorage.server === undefined ? [] : JSON.parse(sessionStorage.server);

  if (data.length === 0 || data.currentTabletOperationResults === undefined) {
    clearAllTableCells("currentTabletOps");
  } else {
    var current = data.currentTabletOperationResults;
    $("#currentMinorAvg").html(timeDuration(current.currentMinorAvg * 1000.0));
    $("#currentMinorStdDev").html(timeDuration(current.currentMinorStdDev * 1000.0));
    $("#currentMajorAvg").html(timeDuration(current.currentMajorAvg * 1000.0));
    $("#currentMajorStdDev").html(timeDuration(current.currentMajorStdDev * 1000.0));
  }
}

/**
 * Generates the server results table
 */
function refreshResultsTable() {
  tabletResults.ajax.reload(null, false); // user paging is not reset on reload
}

/*
 * Appends a table cell containing value to the rowId, if value is not null
 */
function appendDurationToRow(rowId, value) {
  let v = EMPTY_CELL;
  if (value != null) {
    v = "<td>" + timeDuration(value * 1000.0) + "</td>";
  }
  $(rowId).append(v);
}
