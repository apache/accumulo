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
    $, MANAGER_SERVER_PROCESS_VIEW, getManagersView, refreshServerInformation, getManagerGoalStateFromSession
*/
"use strict";

const runningBanner = '#managerRunningBanner'
const htmlBanner = '#managerStateBanner'
const htmlBannerMessage = '#manager-banner-message'
const htmlTable = '#managers'
const visibleColumnFilter = (col) => col != "Server Type" && !col.startsWith("accumulo.compaction.") &&
  !col.startsWith("accumulo.fate.");

const fateHtmlTable = '#managers_fate'
const fateVisibleColumnFilter = (col) => col == "Last Contact" || col == "Resource Group" ||
  col == "Server Address" || col.startsWith("accumulo.fate.");

const compactionHtmlTable = '#managers_compactions'
const compactionVisibleColumnFilter = (col) => col == "Last Contact" || col == "Resource Group" ||
  col == "Server Address" || col.startsWith("accumulo.compaction.");

function updateManagerGoalStateBanner() {
  getManagersView().always(function () {
    const goalState = getManagerGoalStateFromSession();
    if (goalState === 'SAFE_MODE' || goalState === 'CLEAN_STOP') {
      $(htmlBannerMessage).text('Manager goal state: ' + goalState);
      $(htmlBanner).show();
    } else {
      $(htmlBanner).hide();
    }
  });
}

function refreshManagerBanners() {
  var mgrsView = getManagersView();
  if (mgrsView.data.length === 0) {
    // show the manager error banner and hide manager table
    $(runningBanner).show();
    $(htmlTable).hide();
    $(fateHtmlTable).hide();
    $(htmlTable).hide();
    $(compactionHtmlTable).hide();
  } else {
    // otherwise, hide the error banner and show manager table
    $(runningBanner).hide();
    $(fateHtmlTable).show();
    $(htmlTable).show();
    $(compactionHtmlTable).show();
  }
  updateManagerGoalStateBanner();
}

function refresh() {
  refreshManagerBanners();
  refreshServerInformation(getManagersView, htmlTable, MANAGER_SERVER_PROCESS_VIEW, htmlBanner, htmlBannerMessage, visibleColumnFilter);
  refreshServerInformation(getManagersView, fateHtmlTable, MANAGER_SERVER_PROCESS_VIEW, htmlBanner, htmlBannerMessage, fateVisibleColumnFilter);
  refreshServerInformation(getManagersView, compactionHtmlTable, MANAGER_SERVER_PROCESS_VIEW, htmlBanner, htmlBannerMessage, compactionVisibleColumnFilter);
}

$(function () {
  sessionStorage[MANAGER_SERVER_PROCESS_VIEW] = JSON.stringify({
    data: [],
    columns: [],
    status: null
  });

  refreshServerInformation(getManagersView, htmlTable, MANAGER_SERVER_PROCESS_VIEW, htmlBanner, htmlBannerMessage, visibleColumnFilter);
  refreshServerInformation(getManagersView, fateHtmlTable, MANAGER_SERVER_PROCESS_VIEW, htmlBanner, htmlBannerMessage, fateVisibleColumnFilter);
  refreshServerInformation(getManagersView, compactionHtmlTable, MANAGER_SERVER_PROCESS_VIEW, htmlBanner, htmlBannerMessage, compactionVisibleColumnFilter);
  refreshManagerBanners
});





// TODO: 6106 - left code commented for the recovery list table to be re-added

/* JSLint global definitions */
/*global
    $, document, sessionStorage, getManager, bigNumberForQuantity,
    timeDuration, dateFormat, getStatus, ajaxReloadTable, getManagerGoalStateFromSession
*/
/*
"use strict";

var managerStatusTable, recoveryListTable, managerStatus;



*/
/**
 * Populates tables with the new information
 */
/*
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
*/
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
/*
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
*/
