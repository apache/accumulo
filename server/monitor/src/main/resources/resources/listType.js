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
var type, minutes;

/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshListType() {
  $.ajaxSetup({
    async: false
  });
  getTraceOfType(type, minutes);
  $.ajaxSetup({
    async: true
  });
  refreshTypeTraceTable(minutes);
}

/**
 * Used to set the refresh interval to 5 seconds
 */
function refresh() {
  clearInterval(TIMER);
  if (sessionStorage.autoRefresh == 'true') {
    TIMER = setInterval('refreshListType()', 5000);
  }
}

/**
 * Generates the trace per type table
 *
 * @param {string} minutes Minutes to display the trace
 */
function refreshTypeTraceTable(minutes) {
  clearTable('trace');

  /*
   * Get the trace type value obtained earlier,
   * if it doesn't exists, create an empty array
   */
  var data = sessionStorage.traceType === undefined ?
      [] : JSON.parse(sessionStorage.traceType);
  /*
   * If the data is empty, create an empty row, otherwise,
   * create the rows for the table
   */
  if (data.length === 0 || data.traces.length === 0) {
    var items = [];
    items.push('<td class="center" colspan="3"><i>No traces in the last ' +
        minutes + ' minute(s)</i></td>');
    $('<tr/>', {
      html: items.join('')
    }).appendTo('#trace');
  } else {
    $.each(data.traces, function(key, val) {
      var items = [];

      // Convert start value to a date
      var date = new Date(val.start);
      items.push('<td class="firstcell left"><a href="/trace/show?id=' +
          val.id + '">' + date.toLocaleString() + '</a></td>');
      items.push('<td class ="right">' + timeDuration(val.ms) + '</td>');
      items.push('<td class="left">' + val.source + '</td>');

      $('<tr/>', {
        html: items.join('')
      }).appendTo('#trace');
    });
  }
}

/**
 * Sorts the trace table on the selected column
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
  sortTables('trace', direction, n);
}

/**
 * Create tooltip for table column information
 */
$(function() {
  $(document).tooltip();
});

/**
 * Creates the trace type header
 *
 * @param {string} type2 Trace type to display
 * @param {string} minutes2 Minutes to display trace
 */
function createHeader(type2, minutes2) {
  var caption = [];
  type = type2;
  minutes = minutes2;

  caption.push('<span class="table-caption">Traces for ' +
      type + '</span><br />');

  $('<caption/>', {
    html: caption.join('')
  }).appendTo('#trace');

  var items = [];

  /*
   * Adds the columns, add sortTable function on click,
   * if the column has a description, add title taken from the global.js
   */
  items.push('<th class="firstcell" title="' + descriptions['Trace Start'] +
      '">Start&nbsp;</th>');
  items.push('<th title="' + descriptions['Span Time'] + '">ms&nbsp;</th>');
  items.push('<th title="' + descriptions['Source'] + '">Source&nbsp;</th>');

  $('<tr/>', {
    html: items.join('')
  }).appendTo('#trace');
}
