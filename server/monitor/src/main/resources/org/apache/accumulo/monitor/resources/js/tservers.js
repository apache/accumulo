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
var recoveryList = [];

/**
 * Checks if the given server is in the global recoveryList variable
 * 
 * @param {JSON} server json server object
 * @returns true if the server is in the recoveryList, else false
 */
function serverIsInRecoveryList(server) {
  return recoveryList.includes(server.hostname);
}

/**
 * Refreshes the list of recovering tservers and shows/hides the recovery caption
 */
function refreshRecoveryList() {
  getRecoveryList().then(function () {
    var sessionStorageRecoveryList, sessionStorageTserversList;

    // get list of recovering servers and online servers from sessionStorage
    sessionStorageRecoveryList = sessionStorage.recoveryList === undefined ? [] : JSON.parse(sessionStorage.recoveryList).recoveryList;
    sessionStorageTserversList = sessionStorage.tservers === undefined ? [] : JSON.parse(sessionStorage.tservers).servers;

    // update global recovery list variable
    recoveryList = sessionStorageRecoveryList.map(function (entry) {
      return entry.server;
    });

    // show the recovery caption if any online servers are in the recovery list
    if (sessionStorageTserversList.some(serverIsInRecoveryList)) {
      $('#recovery-caption').show();
    } else {
      $('#recovery-caption').hide();
    }
  });
}

/**
 * Show a page banner that matches the tablet server status shown in the navbar.
 */
function refreshTServersBanner() {
  var statusData = JSON.parse(sessionStorage.status);
  if (statusData.managerStatus === 'ERROR') {
    $('#tserversManagerBanner').show();
    $('#tserversWarnBanner').hide();
    $('#tserversErrorBanner').hide();
    $('#tservers_wrapper').hide();
    $('#recovery-caption').hide();
  } else {
    $('#tserversManagerBanner').hide();
    $('#tservers_wrapper').show();
    if (statusData.tServerStatus === 'ERROR') {
      $('#tserversWarnBanner').hide();
      $('#tserversErrorBanner').show();
    } else if (statusData.tServerStatus === 'WARN') {
      $('#tserversWarnBanner').show();
      $('#tserversErrorBanner').hide();
    } else {
      $('#tserversWarnBanner').hide();
      $('#tserversErrorBanner').hide();
    }
  }
}


function refresh() {
  refreshRecoveryList();
  refreshTServersBanner();
  refreshServerInformation(getTserversView, htmlTable, TABLET_SERVER_PROCESS_VIEW, htmlBanner,
    htmlBannerMessage);
}

$(function () {

  refreshRecoveryList();

  sessionStorage[TABLET_SERVER_PROCESS_VIEW] = JSON.stringify({
    data: [],
    columns: [],
    status: null
  });

  refreshServerInformation(getTserversView, htmlTable, TABLET_SERVER_PROCESS_VIEW, htmlBanner,
    htmlBannerMessage);
});
