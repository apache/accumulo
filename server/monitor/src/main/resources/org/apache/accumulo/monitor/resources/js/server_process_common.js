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
    renderActivityState, renderMemoryState, COLUMN_MAP
*/
"use strict";

var dataTableRef;

function getStoredView(storageKey) {
  if (!sessionStorage[storageKey]) {
    return {};
  }
  return JSON.parse(sessionStorage[storageKey]);
}

function getStoredColumns(storageKey) {
  var view = getStoredView(storageKey);
  if (!Array.isArray(view.columns)) {
    return [];
  }
  return view.columns;
}

function getStoredRows(storageKey) {
  var view = getStoredView(storageKey);
  if (!Array.isArray(view.data)) {
    return [];
  }
  console.debug('table data: ' + JSON.stringify(view.data));
  return view.data;
}

function getStoredStatus(storageKey) {
  var view = getStoredView(storageKey);
  return view.status || null;
}

function getDataTableCols(storageKey) {
  var dataTableColumns = [];
  var storedColumns = getStoredColumns(storageKey);
  $.each(storedColumns, function (index, col) {
    var colName = col.replaceAll(".", "\\.");
    dataTableColumns.push({
      data: colName
    });
  });
  console.debug('table columns: ' + JSON.stringify(dataTableColumns));
  return dataTableColumns;
}

function refreshTable(table, storageKey) {

  // Destroy the DataTable and clear the HTML table
  if (dataTableRef != null) {
    dataTableRef.destroy();
    $(table).empty();
  }

  // Create the HTML table columns
  var htmlTableElement = $(table);
  var thead = $(document.createElement("thead"));
  var theadRow = $(document.createElement("tr"));

  var storedColumns = getStoredColumns(storageKey);
  $.each(storedColumns, function (index, col) {
    console.debug('Adding table header row for column: ' + JSON.stringify(col));
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
  htmlTableElement.append(thead);

  // Create the DataTable
  createDataTable(table, storageKey);
  ajaxReloadTable(dataTableRef);
}

function refreshBanner(banner, bannerMsg, status) {
  if (status && status.level === 'WARN') {
    $(bannerMsg)
      .removeClass('alert-danger')
      .addClass('alert-warning')
      .text(status.message || 'WARN: scan-server status warning.');
    $(banner).show();
  } else {
    $(banner).hide();
  }
}

function showBannerError(banner, bannerMsg) {
  $(bannerMsg)
    .removeClass('alert-warning')
    .addClass('alert-danger')
    .text('ERROR: unable to retrieve scan-server status.');
  $(banner).show();
}

function refreshServerInformation(callback, table, storageKey, banner, bannerMsg) {
  callback().then(function () {
    refreshTable(table, storageKey);
    refreshBanner(banner, bannerMsg, getStoredStatus(storageKey));
  }).fail(function () {
    sessionStorage[storageKey] = JSON.stringify({
      data: [],
      columns: [],
      status: null
    });
    refreshTable(table, storageKey);
    showBannerError(banner, bannerMsg);
  });
}

function createDataTable(table, storageKey) {
  dataTableRef = $(table).DataTable({
    "autoWidth": false,
    "ajax": function (data, callback) {
      callback({
        data: getStoredRows(storageKey)
      });
    },
    "stateSave": false, // if set to true, then visible: false doesn't work
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
        "targets": "server-type",
        "visible": false
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
    "columns": getDataTableCols(storageKey)
  });

}
