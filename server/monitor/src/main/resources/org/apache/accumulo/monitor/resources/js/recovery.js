/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
"use strict";

const overviewTableElement = '#recovery-overview';
const tabletRecoveryElement = '#tablets-needing-recovery';
const sortingServersElement = '#servers-sorting';
const replayingServersElement = '#servers-replaying';

var overviewDataTable;
var tabletDataTable;
var sortingDataTable;
var replayingDataTable;

function getOverview() {
  var arr = [];
  var overview = getStoredView(RECOVERY).overview;
  arr.push(overview);
  return arr;
}

function getTablets() {
  return getStoredView(RECOVERY).tabletsNeedingRecovery;
}

function getSorting() {
  return getStoredView(RECOVERY).serversSortingLogs;
}

function getReplaying() {
  return getStoredView(RECOVERY).serversRecoveringTablets;
}

function refresh() {
  $.when(getRecoveryInformation()).then(function () {
    ajaxReloadTable(overviewDataTable);
    ajaxReloadTable(tabletDataTable);
    ajaxReloadTable(sortingDataTable);
    ajaxReloadTable(replayingDataTable);
  }).fail(function () {
    sessionStorage[RECOVERY] = JSON.stringify({
      overview: {
        rootTabletRecovering: false,
        metadataTabletsRecovering: 0,
        userTabletsRecovering: 0
      },
      tabletsNeedingRecovery: [],
      serversRecoveringTablets: [],
      serversSortingLogs: []
    });
    ajaxReloadTable(overviewDataTable);
    ajaxReloadTable(tabletDataTable);
    ajaxReloadTable(sortingDataTable);
    ajaxReloadTable(replayingDataTable);
  });
}

$(function () {
  sessionStorage[RECOVERY] = JSON.stringify({
    overview: {
      rootTabletRecovering: false,
      metadataTabletsRecovering: 0,
      userTabletsRecovering: 0
    },
    tabletsNeedingRecovery: [],
    serversRecoveringTablets: [],
    serversSortingLogs: []
  });

  overviewDataTable = $(overviewTableElement).DataTable({
    "ajax": function (data, callback) {
      callback({
        data: getOverview()
      });
    },
    "lengthChange": false,
    "paging": false,
    "searching": false,
    "stateSave": true,
    "colReorder": true,
    "columnDefs": [{
      targets: '_all',
      defaultContent: '-'
    }],
    "columns": [{
        "data": "rootTabletRecovering"
      },
      {
        "data": "metadataTabletsRecovering"
      },
      {
        "data": "userTabletsRecovering"
      }
    ]
  });

  tabletDataTable = $(tabletRecoveryElement).DataTable({
    "ajax": function (data, callback) {
      callback({
        data: getTablets()
      });
    },
    "stateSave": true,
    "colReorder": true,
    "columnDefs": [{
      targets: '_all',
      defaultContent: '-'
    }],
    "columns": [{
        "data": "tableId"
      },
      {
        "data": "tabletId"
      },
      {
        "data": "tabletDir"
      },
      {
        "data": "location"
      }
    ]
  });

  sortingDataTable = $(sortingServersElement).DataTable({
    "ajax": function (data, callback) {
      callback({
        data: getSorting()
      });
    },
    "stateSave": true,
    "colReorder": true,
    "columnDefs": [{
      targets: '_all',
      defaultContent: '-'
    }],
    "columns": [{
        "data": "server"
      },
      {
        "data": "resourceGroup"
      },
      {
        "data": "type"
      },
      {
        "data": "inProgress"
      },
      {
        "data": "avgProgress",
        "type": "html",
        "render": function (data, type, row, meta) {
          if (type === 'display') {
            if (row.avgProgress < 0) {
              data = '--';
            } else {
              var p = Math.round(Number(row.avgProgress * 100));
              console.log("Compaction progress = %" + p);
              data = '<div class="progress"><div class="progress-bar" role="progressbar" style="min-width: 2em; width:' +
                p + '%;">' + p + '%</div></div>';
            }
          }
          return data;
        }
      },
      {
        "data": "longestDuration",
        "render": function (data, type) {
          if (type === 'display') {
            if (data === null || data === undefined) {
              return '&mdash;';
            }
            data = timeDuration(data);
          }
          return data;
        }
      }
    ]
  });

  replayingDataTable = $(replayingServersElement).DataTable({
    "ajax": function (data, callback) {
      callback({
        data: getReplaying()
      });
    },
    "stateSave": true,
    "colReorder": true,
    "columnDefs": [{
      targets: '_all',
      defaultContent: '-'
    }],
    "columns": [{
        "data": "server"
      },
      {
        "data": "resourceGroup"
      },
      {
        "data": "started"
      },
      {
        "data": "completed"
      },
      {
        "data": "failed"
      },
      {
        "data": "inProgress"
      },
      {
        "data": "mutationsReplayed"
      }
    ]
  });

  refresh();

});
