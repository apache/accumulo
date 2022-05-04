/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
"use strict";

var tserversTable;
/**
 * Creates tservers initial table
 */
$(document).ready(function() {

  // hide the note about highlighted rows by default
  $('#recovery-caption').hide();

  const recoveryList = []
  getRecoveryList().then(function () {
    // fill recoveryList
    JSON.parse(sessionStorage.recoveryList).recoveryList.forEach(entry => {
      recoveryList.push(entry.server);
    });
  });
    
    // Create a table for tserver list
    tserversTable = $('#tservers').DataTable({
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
      ],
      "rowCallback": function (row, data, index) {
        // reset background of each row
        $(row).css('background-color', '');

        // return if the current row's tserver is not recovering
        if (!recoveryList.includes(data.hostname))
          return;

        // only show the caption if we know there are rows in the tservers table
        $('#recovery-caption').show();

        // highlight current row
        console.log('Highlighting row index:' + index + ' tserver:' + data.hostname);
        $(row).css('background-color', 'gold');
      }
    });
    refreshTServers();
});

/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshTServers() {
  getTServers().then(function() {
    refreshBadTServersTable();
    refreshDeadTServersTable();
    refreshTServersTable();
  });
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

  clearTableBody('badtservers');

  if (data.length === 0 || data.badServers.length === 0) {
    $('#badtservers').hide();
  } else {
    $('#badtservers').show();
    $.each(data.badServers, function(key, val) {
      var items = [];
      items.push(createFirstCell(val.id, val.id));
      items.push(createRightCell(val.status, val.status));

      $('<tr/>', {
        html: items.join('')
      }).appendTo('#badtservers tbody');
    });
  }
}

/**
 * Generates the deadtservers rows
 */
function refreshDeadTServersTable() {
  var data = sessionStorage.tservers === undefined ?
      [] : JSON.parse(sessionStorage.tservers);

  clearTableBody('deadtservers');

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
      }).appendTo('#deadtservers tbody');
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
  if (tserversTable) tserversTable.ajax.reload(null, false); // user paging is not reset on reload
}
