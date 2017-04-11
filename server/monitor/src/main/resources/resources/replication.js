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
 * Creates replication initial table
 */
$(document).ready(function() {
  createHeader();
  refreshReplication();
});

/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshReplication() {
  $.ajaxSetup({
    async: false
  });
  getReplication();
  $.ajaxSetup({
    async: true
  });
  refreshReplicationsTable();
}

/**
 * Used to redraw the page
 */
function refresh() {
  refreshReplication();
}

/**
 * Generates the replication table
 */
function refreshReplicationsTable() {
  clearTable('replicationStats');

  var data = sessionStorage.replication === undefined ?
      [] : JSON.parse(sessionStorage.replication);

  if (data.length === 0) {
    var items = [];
    items.push(createEmptyRow(5, 'Replication table is offline'));
    $('<tr/>', {
      html: items.join('')
    }).appendTo('#replicationStats');
  } else {
    $.each(data, function(key, val) {
      var items = [];
      items.push(createFirstCell(val.tableName, val.tableName));

      items.push(createRightCell(val.peerName, val.peerName));

      items.push(createRightCell(val.remoteIdentifier, val.remoteIdentifier));

      items.push(createRightCell(val.replicaSystemType, val.replicaSystemType));

      items.push(createRightCell(val.filesNeedingReplication,
          bigNumberForQuantity(val.filesNeedingReplication)));

      $('<tr/>', {
        html: items.join('')
      }).appendTo('#replicationStats');

    });
  }
}

/**
 * Sorts the replicationStats table on the selected column
 *
 * @param {number} n Column number to sort by
 */
function sortTable(n) {

  if (sessionStorage.tableColumnSort !== undefined &&
      sessionStorage.tableColumnSort == n &&
      sessionStorage.direction !== undefined) {
    direction = sessionStorage.direction === 'asc' ? 'desc' : 'asc';
  } else {
    direction = sessionStorage.direction === undefined ?
        'asc' : sessionStorage.direction;
  }
  sessionStorage.tableColumnSort = n;
  sortTables('replicationStats', direction, n);
}

/**
 * Creates the replication header
 */
function createHeader() {
  var caption = [];

  caption.push('<span class="table-caption">Replication Status</span><br>');

  $('<caption/>', {
    html: caption.join('')
  }).appendTo('#replicationStats');

  var items = [];

  var columns = ['Table&nbsp;', 'Peer&nbsp;', 'Remote&nbsp;Identifier&nbsp;',
      'Replica&nbsp;System&nbsp;Type&nbsp;',
      'Files&nbsp;needing&nbsp;replication&nbsp;'];
  /*
   * Adds the columns, add sortTable function on click
   */
  for (i = 0; i < columns.length; i++) {
    var first = i == 0 ? true : false;
    items.push(createHeaderCell(first, 'sortTable(' + i + ')', '', columns[i]));
  }

  $('<tr/>', {
    html: items.join('')
  }).appendTo('#replicationStats');
}
