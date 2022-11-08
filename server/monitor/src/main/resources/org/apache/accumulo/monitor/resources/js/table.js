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

var tableServersTable;

/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshTable() {
  ajaxReloadTable(tableServersTable);
}

/**
 * Used to redraw the page
 */
function refresh() {
  refreshTable();
}

function getQueuedAndRunning(data) {
  return `${data.running}(${data.queued})`;
}

/**
 * Initialize the table
 * 
 * @param {String} tableID the accumulo table ID
 */
function initTableServerTable(tableID) {

  const url = '/rest/tables/' + tableID;
  console.debug('REST url used to fetch data for table.js DataTable: ' + url);

  tableServersTable = $('#participatingTServers').DataTable({
    "ajax": {
      "url": url,
      "dataSrc": "servers"
    },
    "stateSave": true,
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
      },
      {
        "targets": "percent",
        "render": function (data, type) {
          if (type === 'display') {
            data = Math.round(data * 100) + '%';
          }
          return data;
        }
      }
    ],
    "columns": [{
        "data": "hostname",
        "type": "html",
        "render": function (data, type, row) {
          if (type === 'display') {
            data = `<a href="/tservers?s=${row.id}">${data}</a>`;
          }
          return data;
        }
      },
      {
        "data": "tablets"
      },
      {
        "data": "lastContact"
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
        "data": "holdtime"
      },
      {
        "data": function (row) {
          return getQueuedAndRunning(row.compactions.scans);
        }
      },
      {
        "data": function (row) {
          return getQueuedAndRunning(row.compactions.minor);
        }
      },
      {
        "data": function (row) {
          return getQueuedAndRunning(row.compactions.major);
        }
      },
      {
        "data": "indexCacheHitRate"
      },
      {
        "data": "dataCacheHitRate"
      },
      {
        "data": "osload"
      }
    ]
  });

  refreshTable();

}
