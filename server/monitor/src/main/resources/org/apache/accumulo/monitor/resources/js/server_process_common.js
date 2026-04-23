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

/**
 * This file contains methods used to display tables on the Monitor's
 * pages for server processes. The REST Endpoint /rest-v2/servers/view;table=<ServerTable>
 * returns a data structure that has the following format:
 *
 * {
 *   "data": [
 *     {
 *       "colA": "valueA",
 *       "colB": "valueB"
 *     },
 *      {
 *       "colA": "valueA",
 *       "colB": "valueB"
 *      }
 *   ],
 *   "columns": [
 *     {
 *       "key": "colA",
 *       "label": "Column A",
 *       "description": "Description of Column A",
 *       "uiClass": "big-num"
 *     },
 *     {
 *       "key": "colB",
 *       "label": "Column B",
 *       "description": "Description of Column B",
 *       "uiClass": ""
 *     }
 *   ],
 *   "status": {
 *   },
 *   timestamp: long
 * }
 *
 * The value for the 'columns' key is an array of column definitions. The value for the
 * 'data' key is an array of row objects keyed by the column 'key' values.
 *
 * The 'columns' array is used to dynamically create table header rows in the html and
 * the 'data' object is directly consumed by the DataTable where each object in the 'data'
 * is a row in the table and each field in the object is a column.
 *
 * The entrypoint for using the methods in this file is the refreshServerInformation method.
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
 * It retrieves the exact columns from the cached response and creates
 * the DataTables column definitions from them.
 */
function getDataTableCols(storageKey) {
  var dataTableColumns = [];
  var storedColumns = getStoredColumns(storageKey);
  $.each(storedColumns, function (index, col) {
    var colName = col.key.replaceAll(".", "\\.");
    dataTableColumns.push({
      data: colName,
      title: col.label,
      className: col.uiClass || ''
    });
  });
  return dataTableColumns;
}

/**
 * This function refreshes the table. It destroys the DataTable
 * and clears the HTML table. Then it recreates the table header
 * HTML elements from the columns, redefines the DataTable,
 * and executes an ajax method to load the data.
 */
function refreshTable(table, storageKey) {

  // Destroy the DataTable
  var dataTableRef = dataTableRefs.get(table);
  if (dataTableRef != null) {
    dataTableRef.destroy();
  }

  // Preserve table structure (e.g. the title) from the template but rebuild header and body content on refresh
  $(table).find('thead').remove();
  $(table).find('tbody').remove();

  // Create the HTML table columns
  var htmlTableElement = $(table);
  var thead = $(document.createElement("thead"));
  var theadRow = $(document.createElement("tr"));

  var storedColumns = getStoredColumns(storageKey);
  $.each(storedColumns, function (index, col) {
    var th = $(document.createElement("th"));
    th.addClass(col.uiClass || '');
    th.text(col.label);
    th.attr("title", col.description || col.label);
    theadRow.append(th);
  });
  thead.append(theadRow);
  htmlTableElement.append(thead);
  htmlTableElement.append($(document.createElement('tbody')));

  // Create the DataTable
  dataTableRef = createDataTable(table, storageKey);
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
 *
 * callback - the method to use to invoke the REST API call to get the data
 * table - reference to HTML table object in which to create table header columns
 * storageKey - the session storage key for the data returned from the REST API
 * banner - reference to the HTML table that displays a banner
 * bannerMsg - reference to the HTML object that is the banner
 */
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

/**
 * This function creates the DataTable using the 'data' and
 * 'columns' from the response object. It also defines how
 * certain columns should be rendered based on the columns
 * css class.
 */
function createDataTable(table, storageKey) {
  var dataTableRef = $(table).DataTable({
    "autoWidth": false,
    "ajax": function (data, callback) {
      callback({
        data: getStoredRows(storageKey)
      });
    },
    "stateSave": true,
    "colReorder": true,
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
        "targets": "rate-num",
        "render": function (data, type) {
          if (type === 'display') {
            if (data === null || data === undefined) {
              return '&mdash;';
            }
            data = bigNumberForQuantity(data).toString()+"/s";
          }
          return data;
        }
      },
      {
        "targets": "rate-size",
        "render": function (data, type) {
          if (type === 'display') {
            if (data === null || data === undefined) {
              return '&mdash;';
            }
            data = bigNumberForSize(data).toString()+"/s";
          }
          return data;
        }
      },
      {
        "targets": "start-date",
        "render": function (data, type, row) {
          if (type === 'display') {
            if (data === null || data === undefined) data = '&mdash;';
            else if (data === 0) data = 'Waiting';
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
            if (data === null || data === undefined || data === 0) data = '&mdash;';
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
            if (data === null || data === undefined) {
              return '&mdash;';
            }
            data = timeDuration(data);
          }
          return data;
        }
      },
      {
        "targets": "percent",
        "render": function (data, type) {
          if (type === 'display') {
            if (data === null || data === undefined) {
              return '&mdash;';
            }
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
    "columns": getDataTableCols(storageKey)
  });
  return dataTableRef;
}
