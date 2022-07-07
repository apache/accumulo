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

var bulkListTable, bulkPerServerTable;

/**
 * Fetches new data and updates DataTables with it
 */
function refreshBulkImport() {
  ajaxReloadTable(bulkListTable);
  ajaxReloadTable(bulkPerServerTable);
}

/**
 * Used to redraw the page
 */
function refresh() {
  refreshBulkImport();
}

/**
 * Initializes the bulk import DataTables
 */
$(document).ready(function () {

  const url = '/rest/bulkImports';
  console.debug('REST url used to fetch data for the DataTables in bulkImport.js: ' + url);

  // Generates the manager bulk import status table
  bulkListTable = $('#bulkListTable').DataTable({
    "ajax": {
      "url": url,
      "dataSrc": "bulkImport"
    },
    "stateSave": true,
    "columns": [{
        "data": "filename",
        "width": "40%"
      },
      {
        "data": "age",
        "width": "45%",
        "render": function (data, type) {
          if (type === 'display' && Number(data) > 0) {
            data = new Date(Number(data));
          } else {
            data = "-";
          }
          return data;
        }
      },
      {
        "data": "state",
        "width": "15%"
      }
    ]
  });

  // Generates the bulkPerServerTable DataTable
  bulkPerServerTable = $('#bulkPerServerTable').DataTable({
    "ajax": {
      "url": url,
      "dataSrc": "tabletServerBulkImport"
    },
    "stateSave": true,
    "columns": [{
        "data": "server",
        "type": "html",
        "render": function (data, type) {
          if (type === 'display') {
            data = `<a href="/tservers?s=${data}">${data}</a>`;
          }
          return data;
        }
      },
      {
        "data": "importSize"
      },
      {
        "data": "oldestAge",
        "render": function (data, type) {
          if (type === 'display' && Number(data) > 0) {
            data = new Date(Number(data));
          } else {
            data = "-";
          }
          return data;
        }
      }
    ]
  });

  refreshBulkImport();

});
