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

const htmlBanner = '#tserversStatusBanner'
const htmlBannerMessage = '#tservers-banner-message'
const htmlTable = '#tservers'
var tserversTable;

/**
 * Show a page banner that matches the tablet server status shown in the navbar.
 */
function refreshTServersBanner() {
  getStatus().then(function () {
    var statusData = getStoredStatusData();
    if (getComponentStatus(statusData, 'MANAGER') === 'ERROR') {
      $('#tserversManagerBanner').show();
      $(htmlBanner).hide();
      $('#tservers_wrapper').hide();
      $('#recovery-caption').hide();
    } else {
      $('#tserversManagerBanner').hide();
      $('#tservers_wrapper').show();
    }
  });
}


function refresh() {
  refreshServerInformation(getTserversView, htmlTable, TABLET_SERVER_PROCESS_VIEW, htmlBanner,
    htmlBannerMessage);
  refreshTServersBanner();
}

$(function () {
  sessionStorage[TABLET_SERVER_PROCESS_VIEW] = JSON.stringify({
    data: [],
    columns: []
  });

  refreshServerInformation(getTserversView, htmlTable, TABLET_SERVER_PROCESS_VIEW, htmlBanner,
    htmlBannerMessage);
  refreshTServersBanner();
});
