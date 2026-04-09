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

var compactorsTable;

/**
 * Shows a red banner with the given message
 */
function showCompactorsBanner(message) {
  $('#compactors-banner-message')
    .removeClass('alert-warning')
    .addClass('alert-danger')
    .text(message);
  $('#compactorsStatusBanner').show();
}

/**
 * Show the error banner when there are no compactors
 */
function updateCompactorsBanner(compactors) {
  if (!Array.isArray(compactors) || compactors.length === 0) {
    showCompactorsBanner('No compactors are currently registered.');
  } else {
    $('#compactorsStatusBanner').hide();
  }
}

$(function () {
  // display datatables errors in the console instead of in alerts
  $.fn.dataTable.ext.errMode = 'throw';

  compactorsTable = $('#compactorsTable').DataTable({
    "autoWidth": false,
    "ajax": function (data, callback) {
      $.ajax({
        "url": contextPath + 'rest-v2/ec/compactors',
        "method": 'GET'
      }).done(function (response) {
        var compactors = Array.isArray(response.compactors) ? response.compactors : [];
        updateCompactorsBanner(compactors);
        callback({
          "data": compactors
        });
      }).fail(function () {
        showCompactorsBanner('Unable to retrieve compactor status.');
        callback({
          "data": []
        });
      });
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
        "data": "server"
      },
      {
        "data": "groupName"
      },
      {
        "data": "lastContact"
      }
    ]
  });
});

function refreshCompactors() {
  if (compactorsTable) {
    ajaxReloadTable(compactorsTable);
  }
}

function refresh() {
  refreshCompactors();
}
