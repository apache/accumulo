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

const coordinatorHtmlTable = '#coordinators';
const queuesHtmlTable = '#coordinator_queues';
const runningTableHtmlTable = '#table_running';
const runningQueueHtmlTable = '#queue_running';

var tableRunning;
var queueRunning;

function getStoredArray(storageKey) {
  if (!sessionStorage[storageKey]) {
    return [];
  }
  return JSON.parse(sessionStorage[storageKey]);
}

function refresh() {
  $.when(getRunningCompactionsByTable(), getRunningCompactionsByGroup(), getCoordinatorQueueView(), getManagersCompactionView()).then(function () {
    refreshTable(coordinatorHtmlTable, MANAGER_COMPACTION_SERVER_PROCESS_VIEW);
    refreshTable(queuesHtmlTable, COORDINATOR_QUEUE_PROCESS_VIEW);
    ajaxReloadTable(tableRunning);
    ajaxReloadTable(queueRunning);
  }).fail(function () {
    sessionStorage[MANAGER_COMPACTION_SERVER_PROCESS_VIEW] = JSON.stringify({
      data: [],
      columns: [],
      status: null
    });
    sessionStorage[COORDINATOR_QUEUE_PROCESS_VIEW] = JSON.stringify({
      data: [],
      columns: [],
      status: null
    });
    sessionStorage[RUNNING_COMPACTIONS_BY_TABLE] = JSON.stringify([]);
    sessionStorage[RUNNING_COMPACTIONS_BY_GROUP] = JSON.stringify([]);
    refreshTable(coordinatorHtmlTable, MANAGER_COMPACTION_SERVER_PROCESS_VIEW);
    refreshTable(queuesHtmlTable, COORDINATOR_QUEUE_PROCESS_VIEW);
    ajaxReloadTable(tableRunning);
    ajaxReloadTable(queueRunning);
    $(coordinatorHtmlTable).hide();
    $(queuesHtmlTable).hide();
    $(runningTableHtmlTable).hide();
    $(runningQueueHtmlTable).hide();
  });
}

$(function () {
  sessionStorage[MANAGER_COMPACTION_SERVER_PROCESS_VIEW] = JSON.stringify({
    data: [],
    columns: [],
    status: null
  });
  sessionStorage[COORDINATOR_QUEUE_PROCESS_VIEW] = JSON.stringify({
    data: [],
    columns: [],
    status: null
  });
  sessionStorage[RUNNING_COMPACTIONS_BY_TABLE] = JSON.stringify([]);
  sessionStorage[RUNNING_COMPACTIONS_BY_GROUP] = JSON.stringify([]);

  // Create a table for scans list
  tableRunning = $(runningTableHtmlTable).DataTable({
    "ajax": function (data, callback) {
      callback({
        data: getStoredArray(RUNNING_COMPACTIONS_BY_TABLE)
      });
    },
    "stateSave": true,
    "colReorder": true,
    "columnDefs": [{
        targets: '_all',
        defaultContent: '-'
      },
      {
        "targets": 0,
        "render": function (data, type, row) {
          if (type === 'display') {
            data = '<a class="link-body-emphasis" href="tables/' + row.tableId + '">' + row.tableName + '</a>';
          }
          return data;
        }
      }
    ],
    "columns": [{
        "data": "tableName"
      },
      {
        "data": "running"
      }
    ]
  });

  queueRunning = $(runningQueueHtmlTable).DataTable({
    "ajax": function (data, callback) {
      callback({
        data: getStoredArray(RUNNING_COMPACTIONS_BY_GROUP)
      });
    },
    "stateSave": true,
    "colReorder": true,
    "columnDefs": [{
      targets: '_all',
      defaultContent: '-'
    }],
    "columns": [{
        "data": "groupId"
      },
      {
        "data": "running"
      }
    ]
  });

  refresh();
});
