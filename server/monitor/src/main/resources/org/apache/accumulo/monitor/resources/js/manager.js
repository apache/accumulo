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
/* JSLint global definitions */
/*global
    $, document, sessionStorage, getManager, bigNumberForQuantity,
    timeDuration, dateFormat, getStatus, ajaxReloadTable
*/
"use strict";

var managerStatusTable, recoveryListTable, managerStatus;

function createManagerTable() {
  // Generates the manager table
  managerStatusTable = $('#managerStatusTable').DataTable({
    "ajax": function (data, callback, settings) {
      $.ajax({
        url: contextPath + 'rest-v2/manager',
        method: 'GET'
      }).done(function (json) {
        callback({
          "data": [json]
        });
      }).fail(function (jqXHR, textStatus, errorThrown) {
        // This is needed if the url is not yet available, but the manager is up. E.g., Short
        // window where a 404 could occur, which would lead to DataTables error/alert w/out fail()
        console.error("DataTables Ajax error :", errorThrown);
        callback({
          "data": []
        });
      });
    },
    "stateSave": true,
    "searching": false,
    "paging": false,
    "info": false,
    "columnDefs": [{
        "targets": "timestamp",
        "render": function (data, type) {
          if (type === 'display') {
            data = dateFormat(data);
          }
          return data;
        }
      },
      {
        "targets": "metrics",
        "orderable": false,
        "render": function () {
          return "<a href=\"rest-v2/manager/metrics\">Metrics</a>";
        }
      }
    ],
    "columns": [{
        "data": "host"
      },
      {
        "data": "resourceGroup"
      },
      {
        "data": "timestamp"
      },
      {
        "data": "metrics"
      }
    ]
  });
}

function refreshManagerBanners() {
  // If manager status is error
  if (managerStatus === 'ERROR') {
    // show the manager error banner and hide manager table
    $('#managerRunningBanner').show();
    $('#managerStatusTable').hide();
  } else {
    // otherwise, hide the error banner and show manager table
    $('#managerRunningBanner').hide();
    $('#managerStatusTable').show();
  }
}

/**
 * Populates tables with the new information
 */
function refreshManagerTables() {
  getStatus().then(function () {
    managerStatus = JSON.parse(sessionStorage.status).managerStatus;
    refreshManagerBanners();
    if (managerStatusTable === undefined && managerStatus !== 'ERROR') {
      // Can happen if the manager is dead on first loading the page, but later comes back online
      // while using auto-refresh
      createManagerTable();
    } else if (managerStatus !== 'ERROR') {
      ajaxReloadTable(managerStatusTable);
    }
    ajaxReloadTable(recoveryListTable);
  });
}

/*
 * The tables.ftl refresh function will do this functionality.
 * If tables are removed from Manager, uncomment this function.
 */
/**
 * Used to redraw the page
 */
/*function refresh() {
  refreshManager();
}*/

/**
 * Creates initial tables
 */
$(function () {

  getStatus().then(function () {
    managerStatus = JSON.parse(sessionStorage.status).managerStatus;
    if (managerStatus !== 'ERROR') {
      createManagerTable();
    }

    // Generates the recovery table
    recoveryListTable = $('#recoveryList').DataTable({
      "ajax": {
        "url": contextPath + 'rest/tservers/recovery',
        "dataSrc": function (data) {
          data = data.recoveryList;
          if (data.length === 0) {
            console.info('Recovery list is empty, hiding recovery table');
            $('#recoveryList_wrapper').hide();
          } else {
            $('#recoveryList_wrapper').show();
          }
          return data;
        }
      },
      "columnDefs": [{
          "targets": "duration",
          "render": function (data, type) {
            if (type === 'display') {
              data = timeDuration(parseInt(data, 10));
            }
            return data;
          }
        },
        {
          "targets": "percent",
          "render": function (data, type) {
            if (type === 'display') {
              data = (data * 100).toFixed(2) + '%';
            }
            return data;
          }
        }
      ],
      "stateSave": true,
      "columns": [{
          "data": "server"
        },
        {
          "data": "log"
        },
        {
          "data": "time"
        },
        {
          "data": "progress"
        }
      ]
    });

    refreshManagerTables();
  });
});
