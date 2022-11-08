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

var scansList;

/**
 * Creates scans initial table
 */
$(document).ready(function () {
  // Create a table for scans list
  scansList = $('#scansList').DataTable({
    "ajax": {
      "url": '/rest/scans',
      "dataSrc": "scans"
    },
    "stateSave": true,
    "dom": 't<"align-left"l>p',
    "columnDefs": [{
        "targets": "duration",
        "render": function (data, type, row) {
          if (type === 'display') data = timeDuration(data);
          return data;
        }
      },
      {
        "targets": "date",
        "render": function (data, type, row) {
          if (type === 'display') data = dateFormat(data);
          return data;
        }
      }
    ],
    "columns": [{
        "data": "server",
        "type": "html",
        "render": function (data, type, row, meta) {
          if (type === 'display') {
            data = '<a href="/tservers?s=' + row.server + '">' + row.server + '</a>';
          }
          return data;
        }
      },
      {
        "data": "scanCount"
      },
      {
        "data": "oldestScan"
      },
      {
        "data": "fetched"
      },
    ]
  });
});


/**
 * Used to redraw the page
 */
function refresh() {
  refreshScansTable();
}

/**
 * Generates the scans table
 */
function refreshScansTable() {
  if (scansList) scansList.ajax.reload(null, false); // user paging is not reset on reload
}
