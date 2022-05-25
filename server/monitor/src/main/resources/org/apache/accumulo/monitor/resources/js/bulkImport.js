/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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
    $, document, sessionStorage, clearTableBody, EMPTY_ROW_THREE_CELLS, EMPTY_CELL, getBulkImports
*/
"use strict";

/**
 * Generates the manager bulk import status table
 */
function refreshBulkImportTable() {
  $("#bulkListTable tbody").html(EMPTY_ROW_THREE_CELLS);

  // Get the bulk import data from the session
  var data = sessionStorage.bulkImports === undefined ? [] : JSON.parse(sessionStorage.bulkImports);

  // If the data is empty, clear table, otherwise populate
  if (data.length === 0 || data.bulkImport.length === 0) {
    return;
  }
  console.log("Populate bulkListTable with " + sessionStorage.bulkImports);
  var tableBodyHtml = "";
  $.each(data.bulkImport, function (key, val) {
    console.log("Append row " + key + " " + JSON.stringify(val) + " to bulkListTable");
    tableBodyHtml += "<tr><td class='firstcell'>" + val.filename + "</td>";
    tableBodyHtml += "<td class='center'>" + new Date(val.age) + "</td>";
    tableBodyHtml += "<td class='center'>" + val.state + "</td></tr>";
  });

  $("#bulkListTable tbody").html(tableBodyHtml);
}

/**
 * Generates the bulkPerServerTable table
 */
function refreshServerBulkTable() {
  $("#bulkPerServerTable tbody").html(EMPTY_ROW_THREE_CELLS);

  // get the bulkImport data from sessionStorage
  var data = sessionStorage.bulkImports === undefined ? [] : JSON.parse(sessionStorage.bulkImports);

  // if data is empty, log an error because that means no tablet servers were found
  if (data.length === 0 || data.tabletServerBulkImport.length === 0) {
    console.error("No tablet servers.");
    return;
  }
  var tableBodyHtml = "";
  $.each(data.tabletServerBulkImport, function (key, val) {
    console.log("Append " + key + " " + JSON.stringify(val) + " to bulkPerServerTable");
    var ageCell = EMPTY_CELL;
    if (val.oldestAge > 0) {
      ageCell = "<td>" + new Date(val.oldestAge) + "</td>";
    }
    tableBodyHtml += "<tr><td><a href='/tservers?s=" + val.server + "'>" + val.server + "</a></td>";
    tableBodyHtml += "<td>" + val.importSize + "</td>";
    tableBodyHtml += ageCell + "</tr>";
  });

  $("#bulkPerServerTable tbody").html(tableBodyHtml);
}

/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshBulkImport() {
  getBulkImports().then(function () {
    refreshBulkImportTable();
    refreshServerBulkTable();
  });
}

/**
 * Creates bulk import initial table
 */
$(document).ready(function () {
  refreshBulkImport();
});

/**
 * Used to redraw the page
 */
function refresh() {
  refreshBulkImport();
}
