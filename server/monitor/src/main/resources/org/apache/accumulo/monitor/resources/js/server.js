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

var serv, tabletResults, detailTable, historyTable, currentTable, resultsTable;

var currentServer = window.location.search.split('=')[1];
var url = '/rest/tservers/' + currentServer;
console.debug('REST url for fetching data: ' + url);

/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshServer() {
  ajaxReloadTable(detailTable);
  ajaxReloadTable(historyTable);
  ajaxReloadTable(currentTable);
  ajaxReloadTable(resultsTable);
}

/**
 * Used to redraw the page
 */
function refresh() {
  refreshServer();
}


$(document).ready(function () {

  // Create a table for details on the current server
  detailTable = $('#tServerDetail').DataTable({
    "ajax": {
      "url": url,
      "dataSrc": function (data) {
        // the data needs to be in an array to work with DataTables
        var arr = [];
        if (data.details === undefined) {
          console.warn('the value of "details" is undefined');
        } else {
          arr = [data.details];
        }

        return arr;
      }
    },
    "stateSave": true,
    "searching": false,
    "paging": false,
    "info": false,
    "columnDefs": [{
      "targets": "big-num",
      "render": function (data, type) {
        if (type === 'display') {
          data = bigNumberForQuantity(data);
        }
        return data;
      }
    }],
    "columns": [{
        "data": "hostedTablets"
      },
      {
        "data": "entries"
      },
      {
        "data": "minors"
      },
      {
        "data": "majors"
      },
      {
        "data": "splits"
      }
    ]
  });

  // Create a table for all time tablet operations
  historyTable = $('#opHistoryDetails').DataTable({
    "ajax": {
      "url": url,
      "dataSrc": "allTimeTabletResults"
    },
    "stateSave": true,
    "searching": false,
    "paging": false,
    "info": false,
    "columnDefs": [{
        "targets": "big-num",
        "render": function (data, type) {
          if (type === 'display') {
            data = bigNumberForQuantity(data);
          }
          return data;
        }
      },
      {
        "targets": "duration",
        "render": function (data, type) {
          if (type === 'display') {
            if (data === null) {
              data = '-';
            } else {
              data = timeDuration(data * 1000.0);
            }
          }
          return data;
        }
      }
    ],
    "columns": [{
        "data": "operation"
      },
      {
        "data": "success"
      },
      {
        "data": "failure"
      },
      {
        "data": "avgQueueTime"
      },
      {
        "data": "queueStdDev"
      },
      {
        "data": "avgTime"
      },
      {
        "data": "stdDev"
      }
    ]
  });

  // Create a table for tablet operations on the current server
  currentTable = $('#currentTabletOps').DataTable({
    "ajax": {
      "url": url,
      "dataSrc": function (data) {
        // the data needs to be in an array to work with DataTables
        var arr = [];
        if (data.currentTabletOperationResults === undefined) {
          console.warn('the value of "currentTabletOperationResults" is undefined');
        } else {
          arr = [data.currentTabletOperationResults];
        }

        return arr;
      }
    },
    "stateSave": true,
    "searching": false,
    "paging": false,
    "info": false,
    "columnDefs": [{
      "targets": "duration",
      "render": function (data, type) {
        if (type === 'display') {
          data = timeDuration(data * 1000.0);
        }
        return data;
      }
    }],
    "columns": [{
        "data": "currentMinorAvg"
      },
      {
        "data": "currentMinorStdDev"
      },
      {
        "data": "currentMajorAvg"
      },
      {
        "data": "currentMajorStdDev"
      }
    ]
  });

  // Create a table for detailed tablet operations
  resultsTable = $('#perTabletResults').DataTable({
    "ajax": {
      "url": url,
      "dataSrc": "currentOperations"
    },
    "stateSave": true,
    "dom": 't<"align-left"l>p',
    "columnDefs": [{
        "targets": "big-num",
        "render": function (data, type) {
          if (type === 'display') {
            data = bigNumberForQuantity(data);
          }
          return data;
        }
      },
      {
        "targets": "duration",
        "render": function (data, type) {
          if (type === 'display') {
            data = timeDuration(data);
          }
          return data;
        }
      }
    ],
    "columns": [{
        "data": "name",
        "type": "html",
        "render": function (data, type, row) {
          if (type === 'display') {
            data = `<a href="/tables/${row.tableID}">${data}</a>`;
          }
          return data;
        }
      },
      {
        "data": "tablet",
        "type": "html",
        "render": function (data, type) {
          if (type === 'display') {
            data = `<code>${data}</code>`;
          }
          return data;
        }
      },
      {
        "data": "entries"
      },
      {
        "data": "ingest"
      },
      {
        "data": "query"
      },
      {
        "data": "minorAvg"
      },
      {
        "data": "minorStdDev"
      },
      {
        "data": "minorAvgES"
      },
      {
        "data": "majorAvg"
      },
      {
        "data": "majorStdDev"
      },
      {
        "data": "majorAvgES"
      }
    ]
  });

  refreshServer();
});
