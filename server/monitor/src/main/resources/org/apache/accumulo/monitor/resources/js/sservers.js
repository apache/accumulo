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
/* JSLint global definitions */
/*global
    $, sessionStorage, timeDuration, bigNumberForQuantity, bigNumberForSize, ajaxReloadTable,
    getSserversView, renderActivityState, renderMemoryState
*/
"use strict";

var sserversTable;
var SSERVERS_VIEW_SESSION_KEY = 'sserversView';

function getStoredView() {
  if (!sessionStorage[SSERVERS_VIEW_SESSION_KEY]) {
    return {};
  }
  return JSON.parse(sessionStorage[SSERVERS_VIEW_SESSION_KEY]);
}

function getStoredRows() {
  var view = getStoredView();
  if (!Array.isArray(view.servers)) {
    return [];
  }
  return view.servers;
}

function getStoredStatus() {
  var view = getStoredView();
  return view.status || null;
}

function refreshScanServersTable() {
  ajaxReloadTable(sserversTable);
}

function refreshSserversBanner(status) {
  if (status && status.level === 'WARN') {
    $('#sservers-banner-message')
      .removeClass('alert-danger')
      .addClass('alert-warning')
      .text(status.message || 'WARN: scan-server status warning.');
    $('#sserversStatusBanner').show();
  } else {
    $('#sserversStatusBanner').hide();
  }
}

function showSserversBannerError() {
  $('#sservers-banner-message')
    .removeClass('alert-warning')
    .addClass('alert-danger')
    .text('ERROR: unable to retrieve scan-server status.');
  $('#sserversStatusBanner').show();
}

function refreshScanServers() {
  getSserversView().then(function () {
    refreshScanServersTable();
    refreshSserversBanner(getStoredStatus());
  }).fail(function () {
    sessionStorage[SSERVERS_VIEW_SESSION_KEY] = JSON.stringify({
      servers: [],
      status: null
    });
    refreshScanServersTable();
    showSserversBannerError();
  });
}

function refresh() {
  refreshScanServers();
}

$(function () {
  sessionStorage[SSERVERS_VIEW_SESSION_KEY] = JSON.stringify({
    servers: [],
    status: null
  });

  sserversTable = $('#sservers').DataTable({
    "autoWidth": false,
    "ajax": function (data, callback) {
      callback({
        data: getStoredRows()
      });
    },
    "stateSave": true,
    "columnDefs": [{
        "targets": "big-num",
        "render": function (data, type) {
          if (type === 'display') {
            if (data === null || data === undefined) {
              return '&mdash;';
            }
            data = bigNumberForQuantity(data);
          }
          return data;
        }
      },
      {
        "targets": "big-size",
        "render": function (data, type) {
          if (type === 'display') {
            if (data === null || data === undefined) {
              return '&mdash;';
            }
            data = bigNumberForSize(data);
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
        "data": "host"
      },
      {
        "data": "resourceGroup"
      },
      {
        "data": "lastContact"
      },
      {
        "data": "openFiles"
      },
      {
        "data": "queries"
      },
      {
        "data": "scannedEntries"
      },
      {
        "data": "queryResults"
      },
      {
        "data": "queryResultBytes"
      },
      {
        "data": "busyTimeouts"
      },
      {
        "data": "reservationConflicts"
      },
      {
        "data": "zombieThreads"
      },
      {
        "data": "serverIdle",
        "render": renderActivityState
      },
      {
        "data": "lowMemoryDetected",
        "render": renderMemoryState
      },
      {
        "data": "scansPausedForMemory"
      },
      {
        "data": "scansReturnedEarlyForMemory"
      }
    ]
  });

  refreshScanServers();
});
