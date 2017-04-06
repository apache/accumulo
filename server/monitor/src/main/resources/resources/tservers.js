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
 * Creates tservers initial table
 */
$(document).ready(function() {
  createHeader();
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
 * Generates the tservers table
 */
function refreshBadTServersTable() {
  var data = sessionStorage.tservers === undefined ?
      [] : JSON.parse(sessionStorage.tservers);

  $('#badtservers tr').remove();
  $('#badtservers caption').remove();

  if (data.length === 0 || data.badServers.length === 0) {

    $('#badtservers').hide();
  } else {

    $('#badtservers').show();

    var caption = [];

    caption.push('<span class="table-caption">Non-Functioning&nbsp;' +
        'Tablet&nbsp;Servers</span><br />');
    caption.push('<span class="table-subcaption">The following tablet' +
        ' servers reported a status other than Online</span><br />');

    $('<caption/>', {
      html: caption.join('')
    }).appendTo('#badtservers');

    var items = [];

    items.push('<th class="firstcell" onclick="sortTable(0,0)">' +
        'Tablet&nbsp;Server&nbsp;</th>');
    items.push('<th onclick="sortTable(0,1)">Tablet&nbsp;Server&nbsp;' +
        'Status&nbsp;</th>');

    $('<tr/>', {
      html: items.join('')
    }).appendTo('#badtservers');

    $.each(data.badServers, function(key, val) {
      var items = [];
      items.push('<td class="firstcell left" data-value="' + val.id +
          '">' + val.id + '</td>');
      items.push('<td class="right" data-value="' + val.status +
          '">' + val.status + '</td>');

      $('<tr/>', {
        html: items.join('')
      }).appendTo('#badtservers');
    });
  }
}

/**
 * Generates the deadtservers table
 */
function refreshDeadTServersTable() {
  var data = sessionStorage.tservers === undefined ?
      [] : JSON.parse(sessionStorage.tservers);

  $('#deadtservers tr').remove();
  $('#deadtservers caption').remove();

  if (data.length === 0 || data.deadServers.length === 0) {

    $('#deadtservers').hide();
  } else {

    $('#deadtservers').show();


    var caption = [];

    caption.push('<span class="table-caption">Dead&nbsp;' +
        'Tablet&nbsp;Servers</span><br />');
    caption.push('<span class="table-subcaption">The following' +
        ' tablet servers are no longer reachable.</span><br />');

    $('<caption/>', {
      html: caption.join('')
    }).appendTo('#deadtservers');

    var items = [];

    items.push('<th class="firstcell" onclick="sortTable(1,0)">' +
        'Server&nbsp;</th>');
    items.push('<th onclick="sortTable(1,1)">Last&nbsp;Updated&nbsp;</th>');
    items.push('<th onclick="sortTable(1,2)">Event&nbsp;</th>');
    items.push('<th>Clear</th>');

    $('<tr/>', {
      html: items.join('')
    }).appendTo('#deadtservers');

    $.each(data.deadServers, function(key, val) {
      var items = [];
      items.push('<td class="firstcell left" data-value="' + val.server +
          '">' + val.server + '</td>');

      var date = new Date(val.lastStatus);
      date = date.toLocaleString().split(' ').join('&nbsp;');
      items.push('<td class="right" data-value="' + val.lastStatus +
          '">' + date + '</td>');
      items.push('<td class="right" data-value="' + val.status +
          '">' + val.status + '</td>');
      items.push('<td class="right"> ' +
          '<a href="javascript:clearDeadTServers(\'' +
          val.server + '\');">clear</a></td>');

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
  var data = sessionStorage.tservers === undefined ?
      [] : JSON.parse(sessionStorage.tservers);

  $('#tservers tr:gt(0)').remove();

  if (data.length === 0 || data.servers.length === 0) {
    var item = '<td class="center" colspan="13"><i>Empty</i></td>';

    $('<tr/>', {
      html: item
    }).appendTo('#tservers');
  } else {

    $.each(data.servers, function(key, val) {
      var items = [];
      items.push('<td class="firstcell left" data-value="' + val.hostname +
          '"><a href="/tservers?s=' + val.id + '">' + val.hostname +
          '</a></td>');

      items.push('<td class="right" data-value="' + val.tablets +
          '">' + bigNumberForQuantity(val.tablets) + '</td>');

      items.push('<td class="right" data-value="' + val.lastContact +
          '">' + timeDuration(val.lastContact) + '</td>');

      items.push('<td class="right" data-value="' + val.entries +
          '">' + bigNumberForQuantity(val.entries) + '</td>');

      items.push('<td class="right" data-value="' + val.ingest +
          '">' + bigNumberForQuantity(Math.floor(val.ingest)) + '</td>');

      items.push('<td class="right" data-value="' + val.query +
          '">' + bigNumberForQuantity(Math.floor(val.query)) + '</td>');

      items.push('<td class="right" data-value="' + val.holdtime +
          '">' + timeDuration(val.holdtime) + '</td>');

      items.push('<td class="right" data-value="' +
          (val.compactions.scans.running + val.compactions.scans.queued) +
          '">' + bigNumberForQuantity(val.compactions.scans.running) +
          '&nbsp;(' + bigNumberForQuantity(val.compactions.scans.queued) +
          ')</td>');

      items.push('<td class="right" data-value="' +
          (val.compactions.minor.running + val.compactions.minor.queued) +
          '">' + bigNumberForQuantity(val.compactions.minor.running) +
          '&nbsp;(' + bigNumberForQuantity(val.compactions.minor.queued) +
          ')</td>');

      items.push('<td class="right" data-value="' +
          (val.compactions.major.running + val.compactions.major.queued) +
          '">' + bigNumberForQuantity(val.compactions.major.running) +
          '&nbsp;(' + bigNumberForQuantity(val.compactions.major.queued) +
          ')</td>');

      items.push('<td class="right" data-value="' +
          val.indexCacheHitRate * 100 + '">' +
          Math.round(val.indexCacheHitRate * 100) +
          '%</td>');

      items.push('<td class="right" data-value="' + val.dataCacheHitRate * 100 +
          '">' + Math.round(val.dataCacheHitRate * 100) +
          '%</td>');

      items.push('<td class="right" data-value="' + val.osload +
          '">' + bigNumberForQuantity(val.osload) + '</td>');

      $('<tr/>', {
        html: items.join('')
      }).appendTo('#tservers');
    });
  }
}

/**
 * Sorts the tservers table on the selected column
 *
 * @param {string} table Table ID to sort
 * @param {number} n Column number to sort by
 */
function sortTable(table, n) {
  var tableIDs = ['badtservers', 'deadtservers', 'tservers'];

  if (sessionStorage.tableColumnSort !== undefined &&
      sessionStorage.tableColumnSort == n &&
      sessionStorage.direction !== undefined) {
    direction = sessionStorage.direction === 'asc' ? 'desc' : 'asc';
  } else {
    direction = sessionStorage.direction === undefined ?
        'asc' : sessionStorage.direction;
  }
  sessionStorage.tableColumn = tableIDs[table];
  sessionStorage.tableColumnSort = n;
  sortTables(tableIDs[table], direction, n);
}

/**
 * Creates the tservers header
 */
function createHeader() {
  var caption = [];

  caption.push('<span class="table-caption">Tablet&nbsp;Servers</span><br />');
  caption.push('<span class="table-subcaption">Click on the ' +
      '<span style="color: #0000ff;">server address</span> to ' +
      'view detailed performance statistics for that server.</span><br />');

  $('<caption/>', {
    html: caption.join('')
  }).appendTo('#tservers');

  var items = [];

  items.push('<th class="firstcell" onclick="sortTable(2,0)">Server&nbsp;</th>');
  items.push('<th onclick="sortTable(2,1)">Hosted&nbsp;Tablets&nbsp;</th>');
  items.push('<th onclick="sortTable(2,2)">Last&nbsp;Contact&nbsp;</th>');
  items.push('<th onclick="sortTable(2,3)" title="' +
      descriptions['Entries'] + '">Entries&nbsp;</th>');
  items.push('<th onclick="sortTable(2,4)" title="' +
      descriptions['Ingest'] + '">Ingest&nbsp;</th>');
  items.push('<th onclick="sortTable(2,5)" title="' +
      descriptions['Query'] + '">Query&nbsp;</th>');
  items.push('<th onclick="sortTable(2,6)" title="' +
      descriptions['Hold Time'] + '">Hold&nbsp;Time&nbsp;</th>');
  items.push('<th onclick="sortTable(2,7)" title="' +
      descriptions['Running Scans'] + '">Running<br />Scans&nbsp;</th>');
  items.push('<th onclick="sortTable(2,8)" title="' +
      descriptions['Minor Compactions'] +
      '">Minor<br />Compactions&nbsp;</th>');
  items.push('<th onclick="sortTable(2,9)" title="' +
      descriptions['Major Compactions'] +
      '">Major<br />Compactions&nbsp;</th>');
  items.push('<th onclick="sortTable(2,10)" title="' +
      descriptions['Index Cache Hit Rate'] +
      '">Index Cache<br />Hit Rate&nbsp;</th>');
  items.push('<th onclick="sortTable(2,11)" title="' +
      descriptions['Data Cache Hit Rate'] +
      '">Data Cache<br />Hit Rate&nbsp;</th>');
  items.push('<th onclick="sortTable(2,12)" title="' +
      descriptions['OS Load'] + '">OS&nbsp;Load&nbsp;</th>');

  $('<tr/>', {
    html: items.join('')
  }).appendTo('#tservers');
}
