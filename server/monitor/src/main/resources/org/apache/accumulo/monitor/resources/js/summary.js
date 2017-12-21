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
    items.push(createEmptyRow(6, 'No traces available for the last ' +
        minutes + ' minute(s)'));
    $('<tr/>', {
      html: items.join('')
    }).appendTo('#traceSummary');
  } else {
    $.each(data.recentTraces, function(key, val) {

      var items = [];

      items.push(createFirstCell('', '<a href="/trace/listType?type=' +
          val.type + '&minutes=' + minutes + '">' + val.type + '</a>'));
      items.push(createRightCell('', bigNumberForQuantity(val.total)));
      items.push(createRightCell('', timeDuration(val.min)));
      items.push(createRightCell('', timeDuration(val.max)));
      items.push(createRightCell('', timeDuration(val.avg)));
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