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

var compactionsList;
/**
 * Creates active compactions table
 */
$(document).ready(function () {
  // Create a table for compactions list
  compactionsList = $('#compactionsList').DataTable({
    "ajax": {
      "url": '/rest/compactions',
      "dataSrc": "compactions"
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
        "data": "count"
      },
      {
        "data": "oldest"
      },
      {
        "data": "fetched"
      },
    ]
  });
});

/**
 * Generates the compactions table
 */
function refreshCompactionsTable() {
  if (compactionsList) compactionsList.ajax.reload(null, false); // user paging is not reset on reload
}

/**
 * Used to redraw the page
 */
function refresh() {
  refreshCompactionsTable();
}
