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

    caption.push('<span class="table-caption">Log&nbsp;Recovery</span><br />');
    caption.push('<span class="table-subcaption">Some tablets were unloaded' +
        ' in an unsafe manner. Write-ahead logs are being' +
        ' recovered.</span><br />');

    $('<caption/>', {
      html: caption.join('')
    }).appendTo('#recoveryList');

    var items = [];

    /*
     * Create the header for the recovery list table
     * Adds the columns, add sortTable function on click,
     * if the column has a description, add title taken from the global.js
     */
    items.push('<th class="firstcell" onclick="sortTable(0)">' +
        'Server&nbsp;</th>');
    items.push('<th onclick="sortTable(1)">Log&nbsp;</th>');
    items.push('<th onclick="sortTable(2)">Time&nbsp;</th>');
    items.push('<th onclick="sortTable(3)">Copy/Sort&nbsp;</th>');

    $('<tr/>', {
      html: items.join('')
    }).appendTo('#recoveryList');

    // Creates the table for the recovery list
    $.each(data.recoveryList, function(key, val) {
      var items = [];
      items.push('<td class="firstcell left" data-value="' + val.server + '">' +
          val.server + '</td>');
      items.push('<td class="right" data-value="' + val.log + '">' + val.log +
          '</td>');
      var date = new Date(parseInt(val.time));
      date = date.toLocaleString().split(' ').join('&nbsp;');
      items.push('<td class="right" data-value="' + val.time + '">' + date +
          '</td>');
      items.push('<td class="right" data-value="' + val.copySort + '">' +
          val.copySort + '</td>');

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
    items.push('<td class="firstcell left" data-value="' + data.master +
        '">' + data.master + '</td>');

    items.push('<td class="right" data-value="' + data.onlineTabletServers +
        '">' + data.onlineTabletServers + '</td>');

    items.push('<td class="right" data-value="' + data.totalTabletServers +
        '">' + data.totalTabletServers + '</td>');

    var date = new Date(parseInt(data.lastGC));
    date = date.toLocaleString().split(' ').join('&nbsp;');
    items.push('<td class="left" data-value="' + data.lasGC +
        '"><a href="/gc">' + date + '</a></td>');

    items.push('<td class="right" data-value="' + data.tablets +
        '">' + bigNumberForQuantity(data.tablets) + '</td>');

    items.push('<td class="right" data-value="' + data.unassignedTablets +
        '">' + bigNumberForQuantity(data.unassignedTablets) + '</td>');

    items.push('<td class="right" data-value="' + data.numentries +
        '">' + bigNumberForQuantity(data.numentries) + '</td>');

    items.push('<td class="right" data-value="' + data.ingestrate +
        '">' + bigNumberForQuantity(Math.round(data.ingestrate)) + '</td>');

    items.push('<td class="right" data-value="' + data.entriesRead +
        '">' + bigNumberForQuantity(Math.round(data.entriesRead)) + '</td>');

    items.push('<td class="right" data-value="' + data.queryrate +
        '">' + bigNumberForQuantity(Math.round(data.queryrate)) + '</td>');

    items.push('<td class="right" data-value="' + data.holdTime +
        '">' + timeDuration(data.holdTime) + '</td>');

    items.push('<td class="right" data-value="' + data.osload +
        '">' + bigNumberForQuantity(data.osload) + '</td>');

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
 * Create tooltip for table column information
 */
$(function() {
  $(document).tooltip();
});

/**
 * Creates the master header
 */
function createHeader() {
  var caption = [];

  caption.push('<span class="table-caption">Master&nbsp;Status</span><br />');

  $('<caption/>', {
    html: caption.join('')
  }).appendTo('#masterStatus');

  var items = [];

  /*
   * Adds the columns, add sortTable function on click,
   * if the column has a description, add title taken from the global.js
   */
  items.push('<th class="firstcell" onclick="sortMasterTable(0)" title="' +
      descriptions['Master'] + '">Master&nbsp;</th>');

  items.push('<th onclick="sortMasterTable(1)" title="' +
      descriptions['# Online Tablet Servers'] +
      '">#&nbsp;Online<br />Tablet&nbsp;Servers&nbsp;</th>');

  items.push('<th onclick="sortMasterTable(2)" title="' +
      descriptions['# Total Tablet Servers'] +
      '">#&nbsp;Total<br />Tablet&nbsp;Servers&nbsp;</th>');

  items.push('<th onclick="sortMasterTable(3)" title="' +
      descriptions['Last GC'] + '">Last&nbsp;GC&nbsp;</th>');

  items.push('<th onclick="sortMasterTable(4)" title="' +
      descriptions['# Tablets'] + '">#&nbsp;Tablets&nbsp;</th>');

  items.push('<th onclick="sortMasterTable(5)">#&nbsp;Unassigned' +
      '<br />Tablets&nbsp;</th>');
  items.push('<th onclick="sortMasterTable(6)" title="' +
      descriptions['Total Entries'] + '">Entries&nbsp;</th>');
  items.push('<th onclick="sortMasterTable(7)" title="' +
      descriptions['Total Ingest'] + '">Ingest&nbsp;</th>');
  items.push('<th onclick="sortMasterTable(8)" title="' +
      descriptions['Total Entries Read'] + '">Entries<br />Read&nbsp;</th>');
  items.push('<th onclick="sortMasterTable(9)" title="' +
      descriptions['Total Entries Returned'] +
      '">Entries<br />Returned&nbsp;</th>');
  items.push('<th onclick="sortMasterTable(10)" title="' +
      descriptions['Max Hold Time'] + '">Hold&nbsp;Time&nbsp;</th>');
  items.push('<th onclick="sortMasterTable(11)" title="' +
      descriptions['OS Load'] + '">OS&nbsp;Load&nbsp;</th>');

  $('<tr/>', {
    html: items.join('')
  }).appendTo('#masterStatus');
}
