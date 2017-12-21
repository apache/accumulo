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