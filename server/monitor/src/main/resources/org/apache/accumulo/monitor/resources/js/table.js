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
 * Used to redraw the page
 */
function refresh() {
  refreshTable();
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
    items.push(createEmptyRow(13, 'Empty'));
    $('<tr/>', {
      html: items.join('')
    }).appendTo('#participatingTServers');
  } else {

    $.each(data.servers, function(key, val) {
      var items = [];
      items.push(createFirstCell(val.hostname, '<a href="/tservers?s=' +
          val.id + '">' + val.hostname + '</a>'));

      items.push(createRightCell(val.tablets,
          bigNumberForQuantity(val.tablets)));

      items.push(createRightCell(val.lastContact,
          timeDuration(val.lastContact)));

      items.push(createRightCell(val.entries,
          bigNumberForQuantity(val.entries)));

      items.push(createRightCell(val.ingest,
          bigNumberForQuantity(Math.floor(val.ingest))));

      items.push(createRightCell(val.query,
          bigNumberForQuantity(Math.floor(val.query))));

      items.push(createRightCell(val.holdtime,
          timeDuration(val.holdtime)));

      items.push(createRightCell((val.compactions.scans.running +
          val.compactions.scans.queued),
          bigNumberForQuantity(val.compactions.scans.running) +
          '&nbsp;(' + bigNumberForQuantity(val.compactions.scans.queued) +
          ')'));

      items.push(createRightCell((val.compactions.minor.running +
          val.compactions.minor.queued),
          bigNumberForQuantity(val.compactions.minor.running) +
          '&nbsp;(' + bigNumberForQuantity(val.compactions.minor.queued) +
          ')'));

      items.push(createRightCell((val.compactions.major.running +
          val.compactions.major.queued),
          bigNumberForQuantity(val.compactions.major.running) +
          '&nbsp;(' + bigNumberForQuantity(val.compactions.major.queued) +
          ')'));

      items.push(createRightCell(val.indexCacheHitRate * 100,
          Math.round(val.indexCacheHitRate * 100) + '%'));

      items.push(createRightCell(val.dataCacheHitRate * 100,
          Math.round(val.dataCacheHitRate * 100) + '%'));

      items.push(createRightCell(val.osload,
          bigNumberForQuantity(val.osload)));

      $('<tr/>', {
        html: items.join('')
      }).appendTo('#participatingTServers');

    });
  }
}
