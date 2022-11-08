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

var gcTable;
/**
 * Creates active compactions table
 */
$(document).ready(function () {
  // Create a table for compactions list
  gcTable = $('#gcActivity').DataTable({
    "ajax": {
      "url": '/rest/gc',
      "dataSrc": "stats"
    },
    "stateSave": true,
    "dom": 't<"align-left"l>p',
    "columnDefs": [{
        "targets": "date",
        "render": function (data, type, row) {
          if (type === 'display' && data > 0) data = dateFormat(data);
          return data;
        }
      },
      {
        "targets": "duration",
        "render": function (data, type, row) {
          if (type === 'display') data = timeDuration(data);
          return data;
        }
      },
      {
        "targets": "big-num",
        "render": function (data, type, row) {
          if (type === 'display') data = bigNumberForQuantity(data);
          return data;
        }
      }
    ],
    "columns": [{
        "data": "type"
      },
      {
        "data": "finished"
      },
      {
        "data": "candidates"
      },
      {
        "data": "deleted"
      },
      {
        "data": "inUse"
      },
      {
        "data": "errors"
      },
      {
        "data": "duration"
      },
    ]
  });
  refreshGCTable();
});

/**
 * Used to redraw the page
 */
function refresh() {
  refreshGCTable();
}

/**
 * Generates the garbage collector table
 */
function refreshGCTable() {
  var status = JSON.parse(sessionStorage.status).gcStatus;

  if (status === 'ERROR') {
    $('#gcBanner').show();
    $('#gcActivity').hide();
  } else {
    $('#gcBanner').hide();
    $('#gcActivity').show();
    if (gcTable) gcTable.ajax.reload(null, false); // user paging is not reset on reload
  }

}

function showBanner() {

}
