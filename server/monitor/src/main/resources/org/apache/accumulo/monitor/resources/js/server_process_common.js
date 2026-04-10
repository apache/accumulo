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

/**
 * This file contains methods used to display tables on the Monitor's
 * pages for server processes. The REST Endpoint /rest-v2/servers/view;serverType=<ServerId.Type>
 * returns a data structure that has the following format:
 * 
 * {
 *   "data": [
 *     {
 *       "colA", "valueA",
 *       "colB", "valueB"
 *     },
 *      {
 *       "colA", "valueA",
 *       "colB", "valueB"
 *      }
 *   ],
 *   "columns": [
 *     "colA",
 *     "colB"
 *   ],
 *   "status": {
 *   },
 *   timestamp: long
 * }
 * 
 * The value for the 'columns' key is an array of strings, where each string is the name
 * of a column. The value for the 'data' key is an array of objects. Each object represents
 * a server and the fields in the object contain the fields specified in the 'columns'
 * array and are in the same order.
 * 
 * The 'columns' array is used to dynamically create table header rows in the html and
 * the 'data' object is directly consumed by the DataTable where each object in the 'data'
 * is a row in the table and each field in the object is a column.
 * 
 * Modify the exclusion list in AbstractServer to remove columns from being returned from
 * the AbstractServer.getMetrics RPC call. Be aware that other pages in the Monitor may
 * use this information, not just the server process pages. To influence which columns
 * are displayed on the server process pages use the column filter.
 */

var dataTableRefs = new Map();

/**
 * This function returns the entire response from session storage
 */
function getStoredView(storageKey) {
  if (!sessionStorage[storageKey]) {
    return {};
  }
  return JSON.parse(sessionStorage[storageKey]);
}

/**
 * This function returns the 'columns' array from the entire response
 */
function getStoredColumns(storageKey) {
  var view = getStoredView(storageKey);
  if (!Array.isArray(view.columns)) {
    return [];
  }
  return view.columns;
}

/**
 * This function returns the 'data' array from the entire response
 */
function getStoredRows(storageKey) {
  var view = getStoredView(storageKey);
  if (!Array.isArray(view.data)) {
    return [];
  }
  console.debug('table data: ' + JSON.stringify(view.data));
  return view.data;
}

/**
 * This function returns the stats object from the entire response
 */
function getStoredStatus(storageKey) {
  var view = getStoredView(storageKey);
  return view.status || null;
}

/**
 * This function is called as part of the DataTable initialization.
 * It retrieves the columns from the response and sets the columns
 * visibility based on the supplied filter. Setting the visiblity
 * to false will hide the column in the display even if the table
 * header HTML column exists. This allows us to create the table
 * header rows without supplying the filter.
 */
function getDataTableCols(storageKey, visibleColumnFilter) {
  var dataTableColumns = [];
  var storedColumns = getStoredColumns(storageKey);
  var visibleColumns = storedColumns.filter(visibleColumnFilter);
  $.each(storedColumns, function (index, col) {
    var v = visibleColumns.includes(col);
    var colName = col.replaceAll(".", "\\.");
    dataTableColumns.push({
      data: colName,
      visible: v
    });
  });
  console.debug('table columns: ' + JSON.stringify(dataTableColumns));
  return dataTableColumns;
}

/**
 * This function refreshes the table. It destroys the DataTable
 * and clears the HTML table. Then it recreates the table header
 * HTML elements from the columns, redefines the DataTable,
 * and executes an ajax method to load the data.
 */
function refreshTable(table, storageKey, visibleColumnFilter) {

  // Destroy the DataTable and clear the HTML table
  var dataTableRef = dataTableRefs.get(table);
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
  dataTableRef = createDataTable(table, storageKey, visibleColumnFilter);
  ajaxReloadTable(dataTableRef);
  dataTableRefs.set(table, dataTableRef);
}

/**
 * This function shows a banner on the page if the
 * status level is WARN in the Status object.
 */
function refreshBanner(banner, bannerMsg, status) {
  if (status && status.level === 'WARN') {
    $(bannerMsg)
      .removeClass('alert-danger')
      .addClass('alert-warning')
      .text(status.message || 'WARN: server status warning.');
    $(banner).show();
  } else {
    $(banner).hide();
  }
}

function showBannerError(banner, bannerMsg) {
  $(bannerMsg)
    .removeClass('alert-warning')
    .addClass('alert-danger')
    .text('ERROR: unable to retrieve server status.');
  $(banner).show();
}

/**
 * This function refreshes the table and banner, showing an
 * empty table and error banner if not successful
 */
function refreshServerInformation(callback, table, storageKey, banner, bannerMsg, visibleColumnFilter) {
  callback().then(function () {
    refreshTable(table, storageKey, visibleColumnFilter);
    refreshBanner(banner, bannerMsg, getStoredStatus(storageKey));
  }).fail(function () {
    sessionStorage[storageKey] = JSON.stringify({
      data: [],
      columns: [],
      status: null
    });
    refreshTable(table, storageKey, visibleColumnFilter);
    showBannerError(banner, bannerMsg);
  });
}

/**
 * This function creates the DataTable using the 'data' and
 * 'columns' from the response object. It also defines how
 * certain columns should be rendered based on the columns
 * css class.
 */
function createDataTable(table, storageKey, visibleColumnFilter) {
  var dataTableRef = $(table).DataTable({
    "autoWidth": false,
    "ajax": function (data, callback) {
      callback({
        data: getStoredRows(storageKey)
      });
    },
    "stateSave": true,
    "columnDefs": [{
        targets: '_all',
        defaultContent: '-'
      },
      {
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
        "targets": "start-date",
        "render": function (data, type, row) {
          if (type === 'display') {
            if (data === 0) data = 'Waiting';
            else if (data > 0) data = dateFormat(data);
            else data = 'Error';
          }
          return data;
        }
      },
      {
        "targets": "end-date",
        "render": function (data, type, row) {
          if (type === 'display') {
            if (data === 0) data = '&mdash;';
            else if (data > 0) data = dateFormat(data);
            else data = 'Error';
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
    "columns": getDataTableCols(storageKey, visibleColumnFilter)
  });
  return dataTableRef;
}
