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
 * Used to redraw the page
 */
function refresh() {
  refreshListType();
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
    items.push(createEmptyRow(3, 'No traces for the last ' +
        minutes + ' minute(s)'));
    $('<tr/>', {
      html: items.join('')
    }).appendTo('#trace');
  } else {
    $.each(data.traces, function(key, val) {
      var items = [];

      // Convert start value to a date
      var date = new Date(val.start);
      items.push(createFirstCell('', '<a href="/trace/show?id=' +
          val.id + '">' + date.toLocaleString() + '</a>'));
      items.push(createRightCell('', timeDuration(val.ms)));
      items.push(createLeftCell('', val.source));

      $('<tr/>', {
        html: items.join('')
      }).appendTo('#trace');
    });
  }
}