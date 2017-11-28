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
 * Makes the REST calls, generates the tables with the new information
 */
function refreshTables() {
  $.ajaxSetup({
    async: false
  });
  getNamespaces();
  $.ajaxSetup({
    async: true
  });
  createNamespacesDropdown();
  // If the namespaces in sessionStorage is undefined, select all namespaces
  if (sessionStorage.namespaces === undefined) {
    sessionStorage.namespaces = '[]';
    populateTable('*');
  }
  populateTable(undefined);
  sortTable(sessionStorage.tableColumnSort === undefined ?
      0 : sessionStorage.tableColumnSort);
}

/**
 * Used to redraw the page
 */
function refresh() {
  // If tables are in master page, refresh master and tables
  if (!hasMaster) {
    refreshTables();
  } else {
    refreshMaster();
    refreshTables();
  }
}

var hasMaster = false;
function toggleMaster(master) {
  hasMaster = master
}

/**
 * Creates listeners for when the namespaces are selected or unselected
 */
function namespaceChanged() {
  var $namespaceSelect = $('#namespaces');

  $namespaceSelect.off();

  $namespaceSelect.on('select2:select', function(e) {
    var id = e.params === null ? undefined : e.params['data']['id'];
    populateTable(id);
  });

  $namespaceSelect.on('select2:unselect', function(e) {
    var id = e.params === null ? undefined : e.params['data']['id'];
    populateTable(id);
  });
}

/**
 * Creates the namespaces dropdown menu
 */
function createNamespacesDropdown() {
  var data = JSON.parse(NAMESPACES).namespaces;
  var caption = [];

  caption.push('<span class="table-caption">Table&nbsp;List</span><br>');

  $('<caption/>', {
    html: caption.join('')
  }).appendTo('#filters');

  var data2 = [{ id: '*', text: '* (All Tables)'}];
  $.each(data, function(key, val) {
    var namespace = val === '' ? '- (DEFAULT)' : val;
    data2.push({id: val === '' ? '-' : val, text: namespace});
  });

  $('#namespaces').select2({
    data: data2,
    allowClear: true
  });
  namespaceChanged();
}

/**
 * Creates the tables table with the selected namespace
 *
 * @param {string} ns Selected namespace
 */
function populateTable(ns) {
  var tmpArr = sessionStorage.namespaces === undefined ?
      [] : JSON.parse(sessionStorage.namespaces);
  sessionStorage.namespaceChanged = true;
  var namespaces = JSON.parse(NAMESPACES).namespaces;

  // If there is a selected namespace, change the displayed tables
  if (ns !== undefined) {
    /*
     * If the namespace has not been selected,
     * add it to the namespace array, otherwise remove it
     */
    if (tmpArr.indexOf(ns) == -1) {
      /* If the namespace is *, add all namespaces to the array,
       * otherwise just add the selected namespace
       */
      if (ns === '*') {
        tmpArr = [];

        tmpArr.push('*');
        $.each(namespaces, function(key, val) {
          tmpArr.push(val === '' ? '-' : val);
        });
      } else {
        tmpArr.push(ns);
        /*
         * If the namespace array is the same size
         * as the total number of namespaces, add *
         */
        if (tmpArr.length == namespaces.length) {
          tmpArr.push('*');
        }
      }
    } else {
      /*
       * If * is in the array, and the selected
       * namespace is not *, remove * from array
       */
      if (tmpArr.indexOf('*') !== -1 && ns !== '*') {
        tmpArr.splice(tmpArr.indexOf('*'), 1);
      }
      /*
       * If the selected namespace is not *,
       * remove it from array, otherwise, remove all
       */
      if (ns !== '*') {
        tmpArr.splice(tmpArr.indexOf(ns), 1);
      } else {
        tmpArr = [];
      }
    }
  }

  $('#namespaces').select2().val(tmpArr).trigger('change'); // TODO Fix this, causes null dataAdapter

  sessionStorage.namespaces = JSON.stringify(tmpArr);

  $.ajaxSetup({
    async: false
  });
  getNamespaceTables(tmpArr);
  $.ajaxSetup({
    async: true
  });

  var data = sessionStorage.tables === undefined ?
      [] : JSON.parse(sessionStorage.tables);
  clearTable('tableList');

  var numTables = 0;

  $.each(data.tables, function(keyT, tab) {
    // Only add tables that are part of the array, or all if * is in the array
    if (tmpArr.indexOf(tab.namespace === '' ? '-' : tab.namespace) !== -1 ||
        tmpArr.indexOf('*') !== -1) {
      $.each(tab.table, function(key, val) {

        var row = [];
        row.push(createFirstCell(val.tablename,
            '<a href="/tables/' + val.tableId + '">' + val.tablename + '</a>'));

        row.push(createCenterCell(val.tableState,
            '<span>' + val.tableState + '</span>'));

        if (val.tableState === 'ONLINE') {
          row.push(createRightCell(val.tablets,
              bigNumberForQuantity(val.tablets)));

          row.push(createRightCell(val.offlineTablets,
              bigNumberForQuantity(val.offlineTablets)));

          row.push(createRightCell(val.recs,
              bigNumberForQuantity(val.recs)));

          row.push(createRightCell(val.recsInMemory,
              bigNumberForQuantity(val.recsInMemory)));

          row.push(createRightCell(val.ingest,
              bigNumberForQuantity(Math.floor(val.ingest))));

          row.push(createRightCell(val.entriesRead,
              bigNumberForQuantity(Math.floor(val.entriesRead))));

          row.push(createRightCell(val.entriesReturned,
              bigNumberForQuantity(Math.floor(val.entriesReturned))));

          row.push(createRightCell(val.holdTime,
              timeDuration(val.holdTime)));

          if (val.scans === null) {
            row.push(createRightCell('-', '-'));
          } else {
            row.push(createRightCell((val.scans.running + val.scans.queued),
                bigNumberForQuantity(val.scans.running) + '&nbsp;(' +
                bigNumberForQuantity(val.scans.queued) + ')'));
          }
          if (val.minorCompactions === null) {
            row.push(createRightCell('-', '-'));
          } else {
            row.push(createRightCell((val.minorCompactions.running +
                val.minorCompactions.queued),
                bigNumberForQuantity(val.minorCompactions.running) +
                '&nbsp;(' + bigNumberForQuantity(val.minorCompactions.queued) +
                ')'));
          }
          if (val.majorCompactions === null) {
            row.push(createRightCell('-', '-'));
          } else {
            row.push(createRightCell((val.majorCompactions.running +
                val.majorCompactions.queued),
                bigNumberForQuantity(val.majorCompactions.running) +
                '&nbsp;(' + bigNumberForQuantity(val.majorCompactions.queued) +
                ')'));
          }
        } else {
          row.push(createRightCell('-', '-'));
          row.push(createRightCell('-', '-'));
          row.push(createRightCell('-', '-'));
          row.push(createRightCell('-', '-'));
          row.push(createRightCell('-', '-'));
          row.push(createRightCell('-', '-'));
          row.push(createRightCell('-', '-'));
          row.push(createRightCell('-', '&mdash;'));
          row.push(createRightCell('-', '-'));
          row.push(createRightCell('-', '-'));
          row.push(createRightCell('-', '-'));
        }

        $('<tr/>', {
          html: row.join(''),
          id: tab.namespace === '' ? '-' : tab.namespace
        }).appendTo('#tableList');

        numTables++;
      });
    }
  });
  /*
   * If there are no selected namespaces,
   * or selected namespaces result in no tables, display empty
   */
  if (numTables === 0) {
    var item = createEmptyRow(13, 'Empty');

    $('<tr/>', {
      html: item
    }).appendTo('#tableList');
  }
}

/**
 * Sorts the tableList table on the selected column
 *
 * @param {number} n Column number to sort by
 */
function sortTable(n) {
  if (!JSON.parse(sessionStorage.namespaceChanged)) {
    if (sessionStorage.tableColumnSort !== undefined &&
        sessionStorage.tableColumnSort == n &&
        sessionStorage.direction !== undefined) {
      direction = sessionStorage.direction === 'asc' ? 'desc' : 'asc';
    } else {
      direction = sessionStorage.direction === undefined ?
          'asc' : sessionStorage.direction;
    }
  } else {
    direction = sessionStorage.direction === undefined ?
        'asc' : sessionStorage.direction;
  }

  sessionStorage.tableColumnSort = n;

  sortTables('tableList', direction, n);
  sessionStorage.namespaceChanged = false;
}

/**
 * Creates the tables header
 */
function createTablesHeader() {
  var items = [];

  var columns = ['Table&nbsp;Name&nbsp;', 'State&nbsp;', '#&nbsp;Tablets&nbsp;',
      '#&nbsp;Offline<br>Tablets&nbsp;', 'Entries&nbsp;',
      'Entries<br>In&nbsp;Memory&nbsp;', 'Ingest&nbsp;',
      'Entries<br>Read&nbsp;', 'Entries<br>Returned&nbsp;',
      'Hold&nbsp;Time&nbsp;', 'Running<br>Scans&nbsp;',
      'Minor<br>Compactions&nbsp;', 'Major<br>Compactions&nbsp;'];

  var titles = ['', '', descriptions['# Tablets'],
      descriptions['# Offline Tablets'], descriptions['Entries'],
      descriptions['Entries in Memory'], descriptions['Ingest'],
      descriptions['Entries Read'], descriptions['Entries Returned'],
      descriptions['Hold Time'], descriptions['Running Scans'],
      descriptions['Minor Compactions'], descriptions['Major Compactions']];

  for (i = 0; i < columns.length; i++) {
    var first = i == 0 ? true : false;
    items.push(createHeaderCell(first, 'sortTable(' + i + ')',
        titles[i], columns[i]));
  }

  $('<tr/>', {
    html: items.join('')
  }).appendTo('#tableList');
}
