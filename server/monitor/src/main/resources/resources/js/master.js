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
 * Creates master initial table
 */
$(document).ready(function() {
  createHeader();
  doBanner('masterBanner', 'danger', 'Master Server Not Running');
  refreshMaster();

  // Create tooltip for table column information
  $(document).tooltip();
});

/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshMaster() {
  $.ajaxSetup({
    async: false
  });
  getMaster();
  getRecoveryList();
  $.ajaxSetup({
    async: true
  });
  refreshMasterTable();
  recoveryList();
}


/*
 * The tables refresh function will do this functionality
 * If tables are removed from Master, uncomment this function
 */
/**
 * Used to redraw the page
 */
/*function refresh() {
  refreshMaster();
}*/

/**
 * Creates recovery list table
 */
function recoveryList() {
  /*
   * Get the recovery value obtained earlier,
   * if it doesn't exists, create an empty array
   */
  var data = sessionStorage.recoveryList === undefined ?
      [] : JSON.parse(sessionStorage.recoveryList);

  $('#recoveryList tr').remove();

  // If there is no recovery list data, hide the table
  if (data.length === 0 || data.recoveryList.length === 0) {
    $('#recoveryList').hide();
  } else {
    $('#recoveryList').show();

    var caption = [];

    caption.push('<span class="table-caption">Log&nbsp;Recovery</span><br>');
    caption.push('<span class="table-subcaption">Some tablets were unloaded' +
        ' in an unsafe manner. Write-ahead logs are being' +
        ' recovered.</span><br>');

    $('<caption/>', {
      html: caption.join('')
    }).appendTo('#recoveryList');

    var items = [];

    var columns = ['Server&nbsp;', 'Log&nbsp;', 'Time&nbsp;', 'Copy/Sort&nbsp;'];

    /*
     * Create the header for the recovery list table
     * Adds the columns, add sortTable function on click,
     * if the column has a description, add title taken from the global.js
     */
    for (i = 0; i < columns.length; i++) {
      var first = i == 0 ? true : false;
      items.push(createHeaderCell(first, 'sortTable(' + i + ')',
          '', columns[i]));
    }

    $('<tr/>', {
      html: items.join('')
    }).appendTo('#recoveryList');

    // Creates the table for the recovery list
    $.each(data.recoveryList, function(key, val) {
      var items = [];
      items.push(createFirstCell(val.server, val.server));
      items.push(createRightCell(val.log, val.log));
      var date = new Date(parseInt(val.time));
      date = date.toLocaleString().split(' ').join('&nbsp;');
      items.push(createRightCell(val.time, date));
      items.push(createRightCell(val.copySort, val.copySort));

      $('<tr/>', {
        html: items.join('')
      }).appendTo('#recoveryList');
    });
  }
}

/**
 * Generates the master table
 */
function refreshMasterTable() {
  // Gets the master status
  var status = JSON.parse(sessionStorage.status).masterStatus;

  // Hide the banner and the master table
  $('#masterBanner').hide();
  $('#masterStatus tr:gt(0)').remove();
  $('#masterStatus').hide();

  // If master status is error, show banner, otherwise, create master table
  if (status === 'ERROR') {
    $('#masterBanner').show();
  } else {
    $('#masterStatus').show();
    var data = JSON.parse(sessionStorage.master);
    var items = [];
    items.push(createFirstCell(data.master, data.master));

    items.push(createRightCell(data.onlineTabletServers,
        data.onlineTabletServers));

    items.push(createRightCell(data.totalTabletServers,
        data.totalTabletServers));

    var date = new Date(parseInt(data.lastGC));
    date = date.toLocaleString().split(' ').join('&nbsp;');
    items.push(createLeftCell(data.lasGC, '<a href="/gc">' + date + '</a>'));

    items.push(createRightCell(data.tablets,
        bigNumberForQuantity(data.tablets)));

    items.push(createRightCell(data.unassignedTablets,
        bigNumberForQuantity(data.unassignedTablets)));

    items.push(createRightCell(data.numentries,
        bigNumberForQuantity(data.numentries)));

    items.push(createRightCell(data.ingestrate,
        bigNumberForQuantity(Math.round(data.ingestrate))));

    items.push(createRightCell(data.entriesRead,
        bigNumberForQuantity(Math.round(data.entriesRead))));

    items.push(createRightCell(data.queryrate,
        bigNumberForQuantity(Math.round(data.queryrate))));

    items.push(createRightCell(data.holdTime, timeDuration(data.holdTime)));

    items.push(createRightCell(data.osload, bigNumberForQuantity(data.osload)));

    $('<tr/>', {
     html: items.join('')
    }).appendTo('#masterStatus');
  }
}

/**
 * Sorts the masterStatus table on the selected column
 *
 * @param {number} n Column number to sort by
 */
function sortMasterTable(n) {
  if (sessionStorage.tableColumnSort !== undefined &&
      sessionStorage.tableColumnSort == n &&
      sessionStorage.direction !== undefined) {
    direction = sessionStorage.direction === 'asc' ? 'desc' : 'asc';
  } else {
    direction = sessionStorage.direction === undefined ?
        'asc' : sessionStorage.direction;
  }
  sessionStorage.tableColumnSort = n;
  sortTables('masterStatus', direction, n);
}

/**
 * Creates the master header
 */
function createHeader() {
  var caption = [];

  caption.push('<span class="table-caption">Master&nbsp;Status</span><br>');

  $('<caption/>', {
    html: caption.join('')
  }).appendTo('#masterStatus');

  var items = [];

  var columns = ['Master&nbsp;', '#&nbsp;Online<br>Tablet&nbsp;Servers&nbsp;',
      '#&nbsp;Total<br>Tablet&nbsp;Servers&nbsp;', 'Last&nbsp;GC&nbsp;',
      '#&nbsp;Tablets&nbsp;', '#&nbsp;Unassigned<br>Tablets&nbsp;',
      'Entries&nbsp;', 'Ingest&nbsp;', 'Entries<br>Read&nbsp;',
      'Entries<br>Returned&nbsp;', 'Hold&nbsp;Time&nbsp;',
      'OS&nbsp;Load&nbsp;'];

  var titles = [descriptions['Master'], descriptions['# Online Tablet Servers'],
      descriptions['# Total Tablet Servers'], descriptions['Last GC'],
      descriptions['# Tablets'], '', descriptions['Total Entries'],
      descriptions['Total Ingest'], descriptions['Total Entries Read'],
      descriptions['Total Entries Returned'], descriptions['Max Hold Time'],
      descriptions['OS Load']];

  /*
   * Adds the columns, add sortTable function on click,
   * if the column has a description, add title taken from the global.js
   */
  for (i = 0; i < columns.length; i++) {
    var first = i == 0 ? true : false;
    items.push(createHeaderCell(first, 'sortMasterTable(' + i + ')',
        titles[i], columns[i]));
  }

  $('<tr/>', {
    html: items.join('')
  }).appendTo('#masterStatus');
}
