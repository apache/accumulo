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

var tableID;
/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshTable() {
  $.ajaxSetup({
    async: false
  });
  getTableServers(tableID);
  $.ajaxSetup({
    async: true
  });
  refreshTableServersTable();
}

/**
 * Used to set the refresh interval to 5 seconds
 */
function refresh() {
  clearInterval(TIMER);
  if (sessionStorage.autoRefresh == 'true') {
    TIMER = setInterval('refreshTable()', 5000);
  }
}

/**
 * Generates the table servers table
 */
function refreshTableServersTable() {
  $('#participatingTServers tr:gt(0)').remove();

  var data = sessionStorage.tableServers === undefined ?
      [] : JSON.parse(sessionStorage.tableServers);

  if (data.length === 0 || data.servers.length === 0) {
    var items = [];
    items.push('<td class="center" colspan="13"><i>Empty</i></td>');
    $('<tr/>', {
      html: items.join('')
    }).appendTo('#participatingTServers');
  } else {

    $.each(data.servers, function(key, val) {
      var items = [];
      items.push('<td class="firstcell left" data-value="' + val.hostname +
          '"><a href="/tservers?s=' + val.id +
          '">' + val.hostname + '</a></td>');

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
          val.indexCacheHitRate * 100 +
          '">' + Math.round(val.indexCacheHitRate * 100) + '%</td>');

      items.push('<td class="right" data-value="' + val.dataCacheHitRate * 100 +
          '">' + Math.round(val.dataCacheHitRate * 100) + '%</td>');

      items.push('<td class="right" data-value="' + val.osload +
          '">' + bigNumberForQuantity(val.osload) + '</td>');

      $('<tr/>', {
        html: items.join('')
      }).appendTo('#participatingTServers');

    });
    if (data.servers.length === 0) {
      var items = [];
      items.push('<td class="center" colspan="13"><i>Empty</i></td>');

      $('<tr/>', {
        html: items.join('')
      }).appendTo('#participatingTServers');
    }
  }
}

/**
 * Sorts the participatingTServers table on the selected column
 *
 * @param {number} n Column number to sort by
 */
function sortTable(n) {
  if (!JSON.parse(sessionStorage.namespaceChanged)) {
    if (sessionStorage.tableColumnSort !== undefined &&
        sessionStorage.tableColumnSort == n &&
        sessionStorage.direction !== undefined) {
      direction = sessionStorage.direction === 'asc' ? 'desc' : 'asc';
    }
  } else {
    direction = sessionStorage.direction === undefined ?
        'asc' : sessionStorage.direction;
  }
  sessionStorage.tableColumnSort = n;
  sortTables('participatingTServers', direction, n);
}

/**
 * Creates the table servers header
 *
 * @param {string} table Table Name
 * @param {string} tabID Table ID
 */
function createHeader(table, tabID) {
  tableID = tabID;
  var caption = [];

  caption.push('<span class="table-caption">Participating&nbsp;' +
      'Tablet&nbsp;Servers</span><br />');
  caption.push('<span class="table-subcaption">' + table + '</span><br />');

  $('<caption/>', {
    html: caption.join('')
  }).appendTo('#participatingTServers');

  var items = [];

  items.push('<th class="firstcell" onclick="sortTable(0)">Server&nbsp;</th>');

  items.push('<th onclick="sortTable(1)">Hosted&nbsp;Tablets&nbsp;</th>');

  items.push('<th onclick="sortTable(2)">Last&nbsp;Contact&nbsp;</th>');

  items.push('<th onclick="sortTable(3)" title="' + descriptions['Entries'] +
      '">Entries&nbsp;</th>');

  items.push('<th onclick="sortTable(4)" title="' + descriptions['Ingest'] +
      '">Ingest&nbsp;</th>');

  items.push('<th onclick="sortTable(5)" title="' + descriptions['Query'] +
      '">Query&nbsp;</th>');

  items.push('<th onclick="sortTable(6)" title="' + descriptions['Hold Time'] +
      '">Hold&nbsp;Time&nbsp;</th>');

  items.push('<th onclick="sortTable(7)" title="' +
      descriptions['Running Scans'] + '">Running<br />Scans&nbsp;</th>');

  items.push('<th onclick="sortTable(8)" title="' +
      descriptions['Minor Compactions'] +
      '">Minor<br />Compactions&nbsp;</th>');

  items.push('<th onclick="sortTable(9)" title="' +
      descriptions['Major Compactions'] +
      '">Major<br />Compactions&nbsp;</th>');

  items.push('<th onclick="sortTable(10)" title="' +
      descriptions['Index Cache Hit Rate'] +
      '">Index Cache<br />Hit Rate&nbsp;</th>');

  items.push('<th onclick="sortTable(11)" title="' +
      descriptions['Data Cache Hit Rate'] +
      '">Data Cache<br />Hit Rate&nbsp;</th>');

  items.push('<th onclick="sortTable(12)" title="' +
      descriptions['OS Load'] + '">OS&nbsp;Load&nbsp;</th>');

  $('<tr/>', {
    html: items.join('')
  }).appendTo('#participatingTServers');
}
