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
var id;
/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshTraceShow() {
  $.ajaxSetup({
    async: false
  });
  getTraceShow(id);
  $.ajaxSetup({
    async: true
  });
  refreshTraceShowTable();
}

/**
 * Used to redraw the page
 */
function refresh() {
  refreshTraceShow();
}

/**
 * Generates the trace show table
 */
function refreshTraceShowTable() {
  clearTable('trace');
  $('#trace caption span span').remove();
  var data = sessionStorage.traceShow === undefined ?
      [] : JSON.parse(sessionStorage.traceShow);

  if (data.traces.length !== 0) {
    var date = new Date(data.start);
    $('#caption').append('<span>' + date.toLocaleString() + '</span>');

    $.each(data.traces, function(key, val) {
      var id = val.spanID.toString(16);
      var items = [];

      items.push('<tr>');
      items.push(createRightCell('', val.time + '+'));
      items.push(createLeftCell('', val.start));
      items.push('<td style="text-indent: ' + val.level + '0px">' +
          val.location + '</td>');
      items.push(createLeftCell('', val.name));

      if (val.addlData.data.length !== 0 ||
          val.addlData.annotations.length !== 0) {

        items.push('<td><input type="checkbox" id="' + id +
            '_checkbox" onclick="toggle(\'' + id + '\')"></td>');
        items.push('</tr>');
        items.push('<tr id="' + id + '" style="display:none">');
        items.push('<td colspan="5">');
        items.push('<table class="table table-bordered table-striped' +
            ' table-condensed">');

        if (val.addlData.data.length !== 0) {
          items.push('<tr><th>Key</th><th>Value</th></tr>');

          $.each(val.addlData.data, function(key2, val2) {
            items.push('<tr><td>' + val2.key + '</td><td>' + val2.value +
                '</td></tr>');
          });
        }

        if (val.addlData.annotations.length !== 0) {
          items.push('<tr><th>Annotation</th><th>Time Offset</th></tr>');

          $.each(val.addlData.annotations, function(key2, val2) {
            items.push('<tr><td>' + val2.annotation + '</td><td>' + val2.time +
                '</td></tr>');
          });
        }

        items.push('</table>');
        items.push('</td>');
      } else {
        items.push('<td></td>');
      }

      items.push('</tr>');

      $('#trace').append(items.join(''));
    });
  } else {
      var items = [];
      items.push('<tr>');
      items.push(createEmptyRow(5, 'No trace information for ID ' + id));
      items.push('</tr>');
      $('#trace').append(items.join(''));
  }

}