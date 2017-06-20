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
 * Creates garbage collector initial table
 */
$(document).ready(function() {
  createHeader();
  doBanner('gcBanner', 'danger', 'Collector is Unavailable');
  refreshGC();
});

/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshGC() {
  $.ajaxSetup({
    async: false
  });
  getGarbageCollector();
  $.ajaxSetup({
    async: true
  });
  refreshGCTable();

  // Create tooltip for table column information
  $(document).tooltip();
}

/**
 * Used to redraw the page
 */
function refresh() {
  refreshGC();
}

/**
 * Generates the garbage collector table
 */
function refreshGCTable() {
  // Checks the status of the garbage collector
  var status = JSON.parse(sessionStorage.status).gcStatus;

  // Hides the banner, removes any rows from the table and hides the table
  $('#gcBanner').hide();
  $('#gcActivity tr:gt(0)').remove();
  $('#gcActivity').hide();

  /* Check if the status of the gc is an error, if so, show banner, otherwise,
   * create the table
   */
  if (status === 'ERROR') {
    $('#gcBanner').show();
  } else {
    $('#gcActivity').show();
    var data = JSON.parse(sessionStorage.gc);

    // Checks if there is a collection activity
    if (data.files.lastCycle.finished <= 0 &&
        data.files.currentCycle.started <= 0 &&
        data.wals.lastCycle.finished <= 0 &&
        data.wals.currentCycle.started <= 0) {
      var item = createEmptyRow(7, 'No Collection Activity');

      $('<tr/>', {
        html: item
      }).appendTo('#gcActivity');
    } else {

      var gc = {'File&nbsp;Collection,&nbsp;Last&nbsp;Cycle' : data.files.lastCycle,
          'File&nbsp;Collection,&nbsp;Running' : data.files.currentCycle,
          'WAL&nbsp;Collection,&nbsp;Last&nbsp;Cycle' : data.wals.lastCycle,
          'WAL&nbsp;Collection,&nbsp;Running' : data.wals.currentCycle};

      $.each(gc, function(key, val) {
        if (val.finished > 0) {
          var items = [];

          items.push(createFirstCell(key, key));

          var date = new Date(val.finished);
          items.push(createRightCell(val.finished, date.toLocaleString()));

          items.push(createRightCell(val.candidates,
              bigNumberForQuantity(val.candidates)));

          items.push(createRightCell(val.deleted,
              bigNumberForQuantity(val.deleted)));

          items.push(createRightCell(val.inUse,
              bigNumberForQuantity(val.inUse)));

          items.push(createRightCell(val.errors,
              bigNumberForQuantity(val.errors)));

          items.push(createRightCell((val.finished - val.started),
              timeDuration(val.finished - val.started)));

          $('<tr/>', {
            html: items.join('')
          }).appendTo('#gcActivity');
        }
      });
    }
  }
}

/**
 * Sorts the garbage collector table on the selected column
 *
 * @param {number} n Column number to sort by
 */
function sortTable(n) {

  if (sessionStorage.tableColumnSort !== undefined &&
      sessionStorage.tableColumnSort == n &&
      sessionStorage.direction !== undefined) {
    direction = sessionStorage.direction === 'asc' ? 'desc' : 'asc';
  } else {
    direction = sessionStorage.direction === undefined ?
        'asc' : sessionStorage.direction;
  }
  sessionStorage.tableColumnSort = n;
  sortTables('gcActivity', direction, n);
}

/**
 * Creates the garbage collector header
 */
function createHeader() {
  var caption = [];

  caption.push('<span class="table-caption">Collection&nbsp;' +
      'Activity</span><br>');

  $('<caption/>', {
    html: caption.join('')
  }).appendTo('#gcActivity');

  var items = [];

  var columns = ['Activity&nbsp;', 'Finished&nbsp;', 'Candidates&nbsp;',
      'Deleted&nbsp;', 'In&nbsp;Use&nbsp;', 'Errors&nbsp;', 'Duration&nbsp;'];

  /*
   * Adds the columns, add sortTable function on click,
   * if the column has a description, add title taken from the global.js
   */
  for (i = 0; i < columns.length; i++) {
    var first = i == 0 ? true : false;
    items.push(createHeaderCell(first, 'sortTable(' + i + ')', '', columns[i]));
  }

  $('<tr/>', {
    html: items.join('')
  }).appendTo('#gcActivity');
}
