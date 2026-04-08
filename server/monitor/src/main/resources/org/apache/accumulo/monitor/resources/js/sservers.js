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
    getSserversView, renderActivityState, renderMemoryState, COLUMN_MAP
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

function getStoredColumns() {
  var view = getStoredView();
  if (!Array.isArray(view.columns)) {
    return [];
  }
  return view.columns;
}

function getStoredRows() {
  var view = getStoredView();
  if (!Array.isArray(view.data)) {
    return [];
  }
  console.log('table data: ' + JSON.stringify(view.data));
  return view.data;
}

function getStoredStatus() {
  var view = getStoredView();
  return view.status || null;
}

function getDataTableCols() {
  var dataTableColumns = [];
  var storedColumns = getStoredColumns();
  $.each(storedColumns, function (index, col) {
    if (COLUMN_MAP.has(col)) {
      var colName = col.replaceAll(".", "\\.");
      var mapping = COLUMN_MAP.get(col);
      dataTableColumns.push({
        data: colName
        //		,
        //        render: mapping.render
      });
    } else {
      dataTableColumns.push({
        data: col
      });
    }
  });
  console.log('table columns: ' + JSON.stringify(dataTableColumns));
  return dataTableColumns;
}

function refreshScanServersTable() {

  // Destroy the DataTable and clear the HTML table
  if (sserversTable != null) {
    sserversTable.destroy();
    $('#sservers').empty();
  }

  // Create the HTML table columns
  var sserversHtmlTable = $('#sservers');
  var thead = $(document.createElement("thead"));
  var theadRow = $(document.createElement("tr"));

  var storedColumns = getStoredColumns();
  $.each(storedColumns, function (index, col) {
    console.log('Adding table header row for column: ' + JSON.stringify(col));
    if (COLUMN_MAP.has(col)) {
      var mapping = COLUMN_MAP.get(col);
      var th = $(document.createElement("th"));
      th.addClass(mapping.classes);
      th.text(mapping.header);
      th.attr("title", mapping.description);
      theadRow.append(th);
    } else {
      var th = $(document.createElement("th"));
      th.text(col);
      th.attr("title", "Unmapped column");
      theadRow.append(th);
    }
  });
  thead.append(theadRow);
  sserversHtmlTable.append(thead);

  // Create the DataTable
  createDataTable();
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
      data: [],
      columns: [],
      status: null
    });
    refreshScanServersTable();
    showSserversBannerError();
  });
}

function refresh() {
  refreshScanServers();
}

function createDataTable() {
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
      },
      {
        "targets": "idle-state",
        "render": renderActivityState
      },
      {
        "targets": "memory-state",
        "render": renderMemoryState
      }
    ],
    "columns": getDataTableCols()
  });

}

$(function () {
  sessionStorage[SSERVERS_VIEW_SESSION_KEY] = JSON.stringify({
    data: [],
    columns: [],
    status: null
  });

  refreshScanServers();
});
