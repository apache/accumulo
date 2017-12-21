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
var tserversList;
/**
 * Creates tservers initial table
 */
$(document).ready(function() {
    // Create a table for tserver list
    tserversList = $('#tservers').DataTable({
      "ajax": {
        "url": '/rest/tservers',
        "dataSrc": "servers"
      },
      "stateSave": true,
      "columnDefs": [
          { "targets": "big-num",
            "render": function ( data, type, row ) {
              if(type === 'display') data = bigNumberForQuantity(data);
              return data;
            }
          },
          { "targets": "duration",
            "render": function ( data, type, row ) {
              if(type === 'display') data = timeDuration(data);
              return data;
            }
          },
          { "targets": "percent",
            "render": function ( data, type, row ) {
              if(type === 'display') data = Math.round(data * 100) + '%';
              return data;
            }
          }
        ],
      "columns": [
        { "data": "hostname",
          "type": "html",
          "render": function ( data, type, row, meta ) {
            if(type === 'display') data = '<a href="/tservers?s=' + row.id + '">' + row.hostname + '</a>';
            return data;
          }
        },
        { "data": "tablets" },
        { "data": "lastContact" },
        { "data": "responseTime" },
        { "data": "entries" },
        { "data": "ingest" },
        { "data": "query" },
        { "data": "holdtime" },
        { "data": "scansCombo" },
        { "data": "minorCombo" },
        { "data": "majorCombo" },
        { "data": "indexCacheHitRate" },
        { "data": "dataCacheHitRate" },
        { "data": "osload" }
      ]
    });
    refreshTServers();

    // Create tooltip for table column information
    $(document).tooltip();
});

/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshTServers() {
  $.ajaxSetup({
    async: false
  });
  getTServers();
  $.ajaxSetup({
    async: true
  });
  refreshBadTServersTable();
  refreshDeadTServersTable();
  refreshTServersTable();
}

/**
 * Used to redraw the page
 */
function refresh() {
  refreshTServers();
}

/**
 * Generates the tservers rows
 */
function refreshBadTServersTable() {
  var data = sessionStorage.tservers === undefined ?
      [] : JSON.parse(sessionStorage.tservers);

  $('#badtservers > tbody').html('');

  if (data.length === 0 || data.badServers.length === 0) {
    $('#badtservers').hide();
  } else {
    $('#badtservers').show();
    var items = [];
    $.each(data.badServers, function(key, val) {
      var items = [];
      items.push(createFirstCell(val.id, val.id));
      items.push(createRightCell(val.status, val.status));

      $('<tr/>', {
        html: items.join('')
      }).appendTo('#badtservers');
    });
  }
}

/**
 * Generates the deadtservers rows
 */
function refreshDeadTServersTable() {
  var data = sessionStorage.tservers === undefined ?
      [] : JSON.parse(sessionStorage.tservers);

  $('#deadtservers > tbody').html('');

  if (data.length === 0 || data.deadServers.length === 0) {
    $('#deadtservers').hide();
  } else {
    $('#deadtservers').show();
    $.each(data.deadServers, function(key, val) {
      var items = [];
      items.push(createFirstCell(val.server, val.server));

      var date = new Date(val.lastStatus);
      date = date.toLocaleString().split(' ').join('&nbsp;');
      items.push(createRightCell(val.lastStatus, date));
      items.push(createRightCell(val.status, val.status));
      items.push(createRightCell('', '<a href="javascript:clearDeadTServers(\'' +
          val.server + '\');">clear</a>'));

      $('<tr/>', {
        html: items.join('')
      }).appendTo('#deadtservers');
    });
  }
}

/**
 * Makes the REST POST call to clear dead table server
 *
 * @param {string} server Dead TServer to clear
 */
function clearDeadTServers(server) {
  clearDeadServers(server);
  refreshTServers();
  refreshNavBar();
}

/**
 * Generates the tserver table
 */
function refreshTServersTable() {
  if(tserversList) tserversList.ajax.reload();
}
