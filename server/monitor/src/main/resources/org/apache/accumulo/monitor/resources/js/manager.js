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

const runningBanner = '#managerRunningBanner'
const htmlBanner = '#managerStatusBanner'
const htmlBannerMessage = '#manager-banner-message'
const managerStateBanner = '#managerStateBanner'
const managerStateBannerMessage = '#manager-state-message'
const htmlTable = '#managers'
const fateHtmlTable = '#managers_fate'

function updateManagerGoalStateBanner() {
  const status = sessionStorage.status ? JSON.parse(sessionStorage.status) : null;
  const goalState = status ? status.managerGoalState : null;
  if (goalState === 'SAFE_MODE' || goalState === 'CLEAN_STOP') {
    $(managerStateBannerMessage)
      .removeClass('alert-danger alert-warning')
      .addClass(goalState === 'CLEAN_STOP' ? 'alert-danger' : 'alert-warning')
      .text('Manager goal state: ' + goalState);
    $(managerStateBanner).show();
  } else {
    $(managerStateBanner).hide();
  }
}

function refreshManagerBanners() {
  var managerRows = getStoredRows(MANAGER_SERVER_PROCESS_VIEW);
  if (!Array.isArray(managerRows) || managerRows.length === 0) {
    $(runningBanner).show();
  } else {
    $(runningBanner).hide();
  }
  updateManagerGoalStateBanner();
}

function refresh() {
  $.when(getManagersView(), getManagersFateView(), getStatus()).then(function () {
    refreshTable(htmlTable, MANAGER_SERVER_PROCESS_VIEW);
    refreshTable(fateHtmlTable, MANAGER_FATE_SERVER_PROCESS_VIEW);
    refreshManagerBanners();
    refreshBanner(htmlBanner, htmlBannerMessage, getStoredStatus(MANAGER_SERVER_PROCESS_VIEW));
  }).fail(function () {
    sessionStorage[MANAGER_SERVER_PROCESS_VIEW] = JSON.stringify({
      data: [],
      columns: []
    });
    sessionStorage[MANAGER_FATE_SERVER_PROCESS_VIEW] = JSON.stringify({
      data: [],
      columns: []
    });
    refreshTable(htmlTable, MANAGER_SERVER_PROCESS_VIEW);
    refreshTable(fateHtmlTable, MANAGER_FATE_SERVER_PROCESS_VIEW);
    $(runningBanner).show();
    $(managerStateBanner).hide();
    showBannerError(htmlBanner, htmlBannerMessage);
  });
}

$(function () {
  sessionStorage[MANAGER_SERVER_PROCESS_VIEW] = JSON.stringify({
    data: [],
    columns: []
  });
  sessionStorage[MANAGER_FATE_SERVER_PROCESS_VIEW] = JSON.stringify({
    data: [],
    columns: []
  });

  refresh();
});
