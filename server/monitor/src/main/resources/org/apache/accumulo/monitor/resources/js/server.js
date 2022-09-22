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
"use strict";

var detailTable, historyTable, currentTable, resultsTable;

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

/**
 * Initializes all of the DataTables for the given hostname
 * 
 * @param {String} serv the tserver hostname
 */
function initServerTables(serv) {

  const url = '/rest/tservers/' + serv;
  console.debug('REST url used to fetch data for server.js DataTables: ' + url);

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
      },
      {
        "data": "timeSpent" // placeholder for percent column, replaced below
      }
    ],
    // calculate and fill percent column each time table is drawn
    "drawCallback": function () {
      var totalTime = 0;
      var api = this.api();

      // calculate total duration of all tablet operations
      api.rows().every(function () {
        totalTime += this.data().timeSpent;
      });

      const percentColumnIndex = 7;
      api.rows().every(function (rowIdx) {
        // calculate the percentage of time taken for each row (each tablet op)
        var currentPercent = (this.data().timeSpent / totalTime) * 100;
        currentPercent = Math.round(currentPercent);
        if (isNaN(currentPercent)) {
          currentPercent = 0;
        }
        // insert the percentage bar into the current row and percent column
        var newData = `<div class="progress"><div class="progress-bar" role="progressbar" style="min-width: 2em; width:${currentPercent}%;">${currentPercent}%</div></div>`
        api.cell(rowIdx, percentColumnIndex).data(newData);
      });
    }
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
}
