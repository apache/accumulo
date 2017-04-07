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
var minutes;
/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshSummary() {
  $.ajaxSetup({
    async: false
  });
  getTraceSummary(minutes);
  $.ajaxSetup({
    async: true
  });
  refreshTraceSummaryTable(minutes);
}

/**
 * Used to redraw the page
 */
function refresh() {
  refreshSummary();
}

/**
 * Generates the trace summary table
 *
 * @param {string} minutes Minutes to display traces
 */
function refreshTraceSummaryTable(minutes) {
  clearTable('traceSummary');

  var data = sessionStorage.traceSummary === undefined ?
      [] : JSON.parse(sessionStorage.traceSummary);

  if (data.length === 0 || data.recentTraces.length === 0) {
    var items = [];
    items.push('<td class="center" colspan="6"><i>No traces available for the last ' +
        minutes + ' minute(s)</i></td>');
    $('<tr/>', {
      html: items.join('')
    }).appendTo('#traceSummary');
  } else {
    $.each(data.recentTraces, function(key, val) {

      var items = [];

      items.push('<td class="firstcell left"><a href="/trace/listType?type=' +
          val.type + '&minutes=' + minutes + '">' + val.type + '</a></td>');
      items.push('<td class ="right">' + bigNumberForQuantity(val.total) +
          '</td>');
      items.push('<td class="right">' + timeDuration(val.min) + '</td>');
      items.push('<td class="right">' + timeDuration(val.max) + '</td>');
      items.push('<td class="right">' + timeDuration(val.avg) + '</td>');
      items.push('<td class="left">');
      items.push('<table style="width: 100%;">');
      items.push('<tr>');

      $.each(val.histogram, function(key2, val2) {
        items.push('<td style="width:5em">' + (val2 == 0 ? '-' : val2) +
            '</td>');
      });
      items.push('</tr>');
      items.push('</table>');
      items.push('</td>');

      $('<tr/>', {
        html: items.join('')
      }).appendTo('#traceSummary');

    });
  }
}

/**
 * Sorts the traceSummary table on the selected column
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
  sortTables('traceSummary', direction, n);
}

/**
 * Creates the trace summary header
 *
 * @param {string} min Minutes to display trace
 */
function createHeader(min) {
  minutes = min;
  var caption = [];

  caption.push('<span class="table-caption">All Traces</span><br />');

  $('<caption/>', {
    html: caption.join('')
  }).appendTo('#traceSummary');

  var items = [];

  items.push('<th class="firstcell" title="' + descriptions['Trace Type'] +
      '">Type&nbsp;</th>');

  items.push('<th title="' + descriptions['Total Spans'] +
      '">Total&nbsp;</th>');

  items.push('<th title="' + descriptions['Short Span'] +
      '">min&nbsp;</th>');

  items.push('<th title="' + descriptions['Long Span'] +
      '">max&nbsp;</th>');

  items.push('<th title="' + descriptions['Avg Span'] +
      '">avg&nbsp;</th>');

  items.push('<th title="' + descriptions['Histogram'] +
      '">Histogram&nbsp;</th>');

  $('<tr/>', {
    html: items.join('')
  }).appendTo('#traceSummary');
}
