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
}

/**
 * Used to set the refresh interval to 5 seconds
 */
function refresh() {
  clearInterval(TIMER);
  if (sessionStorage.autoRefresh == 'true') {
    TIMER = setInterval('refreshGC()', 5000);
  }
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
      var item = '<td class="center" colspan="7"><i>No Collection Activity' +
          '</i></td>';

      $('<tr/>', {
        html: item
      }).appendTo('#gcActivity');
    } else {

      // File Collection Last Cycle row
      if (data.files.lastCycle.finished > 0) {
        var items = [];

        var working = data.files.lastCycle;

        items.push('<td class="firstcell left" data-value="File Collection' +
            ' Last Cycle">File&nbsp;Collection,&nbsp;Last&nbsp;Cycle</td>"');

        var date = new Date(working.finished);
        items.push('<td class="right" data-value="' + working.finished +
            '">' + date.toLocaleString() + '</td>');

        items.push('<td class="right" data-value="' + working.candidates +
            '">' + bigNumberForQuantity(working.candidates) + '</td>');

        items.push('<td class="right" data-value="' + working.deleted +
            '">' + bigNumberForQuantity(working.deleted) + '</td>');

        items.push('<td class="right" data-value="' + working.inUse +
            '">' + bigNumberForQuantity(working.inUse) + '</td>');

        items.push('<td class="right" data-value="' + working.errors +
            '">' + bigNumberForQuantity(working.errors) + '</td>');

        items.push('<td class="right" data-value="' +
            (working.finished - working.started) + '">' +
            timeDuration(working.finished - working.started) + '</td>');

        $('<tr/>', {
          html: items.join('')
        }).appendTo('#gcActivity');
      }

      // File Collection Running row
      if (data.files.currentCycle.started > 0) {
        var items = [];

        var working = data.files.currentCycle;

        items.push('<td class="firstcell left" data-value="File Collection' +
            ' Running">File&nbsp;Collection,&nbsp;Running</td>');

        var date = new Date(working.finished);
        items.push('<td class="right" data-value="' + working.finished +
            '">' + date.toLocaleString() + '</td>');

        items.push('<td class="right" data-value="' + working.candidates +
            '">' + bigNumberForQuantity(working.candidates) + '</td>');

        items.push('<td class="right" data-value="' + working.deleted +
            '">' + bigNumberForQuantity(working.deleted) + '</td>');

        items.push('<td class="right" data-value="' + working.inUse +
            '">' + bigNumberForQuantity(working.inUse) + '</td>');

        items.push('<td class="right" data-value="' + working.errors +
            '">' + bigNumberForQuantity(working.errors) + '</td>');

        items.push('<td class="right" data-value="' +
            (working.finished - working.started) + '">' +
            timeDuration(working.finished - working.started) + '</td>');

        $('<tr/>', {
          html: items.join('')
        }).appendTo('#gcActivity');
      }

      // WAL Collection Last Cycle row
      if (data.wals.lastCycle.finished > 0) {
        var items = [];

        var working = data.wals.lastCycle;

        items.push('<td class="firstcell left" data-value="WAL Collection' +
            ' Last Cycle">WAL&nbsp;Collection,&nbsp;Last&nbsp;Cycle</td>');

        var date = new Date(working.finished);
        items.push('<td class="right" data-value="' + working.finished +
            '">' + date.toLocaleString() + '</td>');

        items.push('<td class="right" data-value="' + working.candidates +
            '">' + bigNumberForQuantity(working.candidates) + '</td>');

        items.push('<td class="right" data-value="' + working.deleted +
            '">' + bigNumberForQuantity(working.deleted) + '</td>');

        items.push('<td class="right" data-value="' + working.inUse +
            '">' + bigNumberForQuantity(working.inUse) + '</td>');

        items.push('<td class="right" data-value="' + working.errors +
            '">' + bigNumberForQuantity(working.errors) + '</td>');

        items.push('<td class="right" data-value="' +
            (working.finished - working.started) + '">' +
            timeDuration(working.finished - working.started) + '</td>');

        $('<tr/>', {
          html: items.join('')
        }).appendTo('#gcActivity');
      }

      // WAL Collection Running row
      if (data.wals.currentCycle.started > 0) {
        var items = [];

        var working = data.wals.currentCycle;

        items.push('<td class="firstcell left" data-value="WAL Collection' +
            ' Running">WAL&nbsp;Collection,&nbsp;Running</td>');

        var date = new Date(working.finished);
        items.push('<td class="right" data-value="' + working.finished +
            '">' + date.toLocaleString() + '</td>');

        items.push('<td class="right" data-value="' + working.candidates +
            '">' + bigNumberForQuantity(working.candidates) + '</td>');

        items.push('<td class="right" data-value="' + working.deleted +
            '">' + bigNumberForQuantity(working.deleted) + '</td>');

        items.push('<td class="right" data-value="' + working.inUse +
            '">' + bigNumberForQuantity(working.inUse) + '</td>');

        items.push('<td class="right" data-value="' + working.errors +
            '">' + bigNumberForQuantity(working.errors) + '</td>');

        items.push('<td class="right" data-value="' +
            (working.finished - working.started) + '">' +
            timeDuration(working.finished - working.started) + '</td>');

        $('<tr/>', {
          html: items.join('')
        }).appendTo('#gcActivity');
      }
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
 * Create tooltip for table column information
 */
$(function() {
  $(document).tooltip();
});

/**
 * Creates the garbage collector header
 */
function createHeader() {
  var caption = [];

  caption.push('<span class="table-caption">Collection&nbsp;' +
      'Activity</span><br />');

  $('<caption/>', {
    html: caption.join('')
  }).appendTo('#gcActivity');

  var items = [];

  /*
   * Adds the columns, add sortTable function on click,
   * if the column has a description, add title taken from the global.js
   */
  items.push('<th class="firstcell" onclick="sortTable(0)">Activity' +
      '&nbsp;</th>');
  items.push('<th onclick="sortTable(1)">Finished&nbsp;</th>');
  items.push('<th onclick="sortTable(2)">Candidates&nbsp;</th>');
  items.push('<th onclick="sortTable(3)">Deleted&nbsp;</th>');
  items.push('<th onclick="sortTable(4)">In&nbsp;Use&nbsp;</th>');
  items.push('<th onclick="sortTable(5)">Errors&nbsp;</th>');
  items.push('<th onclick="sortTable(6)">Duration&nbsp;</th>');

  $('<tr/>', {
    html: items.join('')
  }).appendTo('#gcActivity');
}
