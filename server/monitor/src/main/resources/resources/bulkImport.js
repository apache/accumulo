/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

/**
 * Creates bulk import initial table
 */
$(document).ready(function() {
  createBulkImportHeader();
  createServerBulkHeader();
  refreshBulkImport();

  // Create tooltip for table column information
  $(document).tooltip();
});

/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshBulkImport() {
  $.ajaxSetup({
    async: false
  });
  getBulkImports();
  $.ajaxSetup({
    async: true
  });
  refreshBulkImportTable();
  refreshServerBulkTable();
}

/**
 * Used to redraw the page
 */
function refresh() {
  refreshBulkImport();
}

/**
 * Generates the master bulk import status table
 */
function refreshBulkImportTable() {

  clearTable('masterBulkImportStatus');

  /*
   * Get the bulk import value obtained earlier, if it doesn't exists,
   * create an empty array
   */
  var data = sessionStorage.bulkImports === undefined ?
      [] : JSON.parse(sessionStorage.bulkImports);
  var items = [];

  /* If the data is empty, create an empty row, otherwise,
   * create the rows for the table
   */
  if (data.length === 0 || data.bulkImport.length === 0) {
    items.push(createEmptyRow(3, 'Empty'));
  } else {
    $.each(data.bulkImport, function(key, val) {
      items.push(createFirstCell(val.filename, val.filename));
      items.push(createRightCell(val.age, val.age));
      items.push(createRightCell(val.state, val.state));
    });
  }

  $('<tr/>', {
    html: items.join('')
  }).appendTo('#masterBulkImportStatus');
}

/**
 * Generates the bulk import status table
 */
function refreshServerBulkTable() {

  clearTable('bulkImportStatus');

  /* Get the bulk import value obtained earlier, if it doesn't exists,
   * create an empty array
   */
  var data = sessionStorage.bulkImports === undefined ?
   [] : JSON.parse(sessionStorage.bulkImports);
  var items = [];

  /* If the data is empty, create an empty row, otherwise
   * create the rows for the table
   */
  if (data.length === 0 || data.tabletServerBulkImport.length === 0) {
    items.push(createEmptyRow(3, 'Empty'));
  } else {
    $.each(data.tabletServerBulkImport, function(key, val) {
      items.push(createFirstCell(val.server, '<a href="/tservers?s=' +
          val.server + '">' + val.server + '</a>'));
      items.push(createRightCell(val.importSize, val.importSize));
      items.push(createRightCell(val.oldestAge, (val.oldestAge > 0 ?
          val.oldestAge : '&mdash;')));
    });
  }

  $('<tr/>', {
    html: items.join('')
  }).appendTo('#bulkImportStatus');
}

/**
 * Sorts the bulkImportStatus table on the selected column
 *
 * @param {string} table Table ID to sort
 * @param {number} n Column number to sort by
 */
function sortTable(table, n) {
  var tableIDs = ['bulkImportStatus', 'masterBulkImportStatus'];

  if (sessionStorage.tableColumnSort !== undefined &&
      sessionStorage.tableColumnSort == n &&
      sessionStorage.direction !== undefined) {
    direction = sessionStorage.direction === 'asc' ? 'desc' : 'asc';
  } else {
    direction = sessionStorage.direction === undefined ?
        'asc' : sessionStorage.direction;
  }
  sessionStorage.tableColumn = tableIDs[table];
  sessionStorage.tableColumnSort = n;
  sortTables(tableIDs[table], direction, n);
}

/**
 * Creates the bulk import header
 */
function createBulkImportHeader() {
  var caption = '<span class="table-caption">Bulk&nbsp;Import' +
      '&nbsp;Status</span><br>';

  $('<caption/>', {
    html: caption
  }).appendTo('#masterBulkImportStatus');

  var items = [];

  var columns = ['Directory&nbsp;', 'Age&nbsp;', 'State&nbsp;'];

  var titles = ['', descriptions['Import Age'], descriptions['Import State']];

  /*
   * Adds the columns, add sortTable function on click,
   * if the column has a description, add title taken from the global.js
   */
  for (i = 0; i < columns.length; i++) {
    var first = i == 0 ? true : false;
    items.push(createHeaderCell(first, 'sortTable(1,' + i + ')',
        titles[i], columns[i]));
  }

  $('<tr/>', {
    html: items.join('')
  }).appendTo('#masterBulkImportStatus');
}

/**
 * Creates the bulk import header
 */
function createServerBulkHeader() {
  var caption = [];

  caption.push('<span class="table-caption">TabletServer&nbsp;Bulk&nbsp;' +
      'Import&nbsp;Status</span><br>');

  $('<caption/>', {
    html: caption.join('')
  }).appendTo('#bulkImportStatus');

  var items = [];

  var columns = ['Server&nbsp;', '#&nbsp;', 'Oldest&nbsp;Age&nbsp;'];

  var titles = ['', descriptions['# Imports'], descriptions['Oldest Age']];

  /*
   * Adds the columns, add sortTable function on click,
   * if the column has a description, add title taken from the global.js
   */
  for (i = 0; i < columns.length; i++) {
    var first = i == 0 ? true : false;
    items.push(createHeaderCell(first, 'sortTable(0,' + i + ')',
        titles[i], columns[i]));
  }

  $('<tr/>', {
    html: items.join('')
  }).appendTo('#bulkImportStatus');
}
