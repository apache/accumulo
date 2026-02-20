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
/*global REST_V2_PREFIX */
"use strict";

/**
 * The status options from the servers
 */
const STATUS = {
  WARN: 'WARN',
  OK: 'OK',
  ERROR: 'ERROR'
};

/**
 * The class names of bootstrap notification color classes
 */
const CLASS = {
  WARNING: 'warning',
  NORMAL: 'normal',
  ERROR: 'error'
};

/**
 * Remove other bootstrap color classes and add the given class to the given element
 * @param {string} elementId the element id to update
 * @param {string} status the status of that element. used to set the associated color
 */
function updateElementStatus(elementId, status) {
  const $element = $(`#${elementId}`);

  switch (status) {
  case STATUS.ERROR:
    $element.removeClass(CLASS.NORMAL).removeClass(CLASS.WARNING).addClass(CLASS.ERROR);
    break;
  case STATUS.WARN:
    $element.removeClass(CLASS.NORMAL).removeClass(CLASS.ERROR).addClass(CLASS.WARNING);
    break;
  case STATUS.OK:
    $element.removeClass(CLASS.ERROR).removeClass(CLASS.WARNING).addClass(CLASS.NORMAL);
    break;
  default:
    console.error('Unrecognized status: ' + status);
  }
}

/**
 * Updates the notifications of the servers dropdown notification as well as the individual server notifications.
 * @param {JSON} statusData object containing the status info for the servers
 */
function updateServerNotifications(statusData) {
  const applyStatuses = function (managerGoalState) {
    const isSafeMode = managerGoalState === 'SAFE_MODE';
    const isCleanStop = managerGoalState === 'CLEAN_STOP';

    // setting manager status notification
    if (statusData.managerStatus === STATUS.ERROR || isCleanStop) {
      updateElementStatus('managerStatusNotification', STATUS.ERROR);
    } else if (statusData.managerStatus === STATUS.WARN || isSafeMode) {
      updateElementStatus('managerStatusNotification', STATUS.WARN);
    } else if (statusData.managerStatus === STATUS.OK) {
      updateElementStatus('managerStatusNotification', STATUS.OK);
    } else {
      console.error('Unrecognized manager state: ' + statusData.managerStatus + '. Could not properly set manager status notification.');
    }

    // setting tserver status notification
    if (statusData.tServerStatus === STATUS.OK) {
      updateElementStatus('serverStatusNotification', STATUS.OK);
    } else if (statusData.tServerStatus === STATUS.WARN) {
      updateElementStatus('serverStatusNotification', STATUS.WARN);
    } else {
      updateElementStatus('serverStatusNotification', STATUS.ERROR);
    }

    // setting gc status notification
    if (statusData.gcStatus === STATUS.OK) {
      updateElementStatus('gcStatusNotification', STATUS.OK);
    } else {
      updateElementStatus('gcStatusNotification', STATUS.ERROR);
    }

    // setting scan server status notification
    if (statusData.sServerStatus === STATUS.ERROR) {
      updateElementStatus('sserverStatusNotification', STATUS.ERROR);
    } else if (statusData.sServerStatus === STATUS.WARN) {
      updateElementStatus('sserverStatusNotification', STATUS.WARN);
    } else if (statusData.sServerStatus === STATUS.OK) {
      updateElementStatus('sserverStatusNotification', STATUS.OK);
    } else {
      console.error('Unrecognized scan server state: ' + statusData.sServerStatus +
        '. Could not properly set scan server status notification.');
    }

    // Setting overall servers status notification
    if ((statusData.managerStatus === STATUS.OK && !isSafeMode && !isCleanStop) &&
      statusData.tServerStatus === STATUS.OK &&
      statusData.gcStatus === STATUS.OK &&
      statusData.sServerStatus === STATUS.OK) {
      updateElementStatus('statusNotification', STATUS.OK);
    } else if (statusData.managerStatus === STATUS.ERROR || isCleanStop ||
      statusData.tServerStatus === STATUS.ERROR ||
      statusData.gcStatus === STATUS.ERROR ||
      statusData.sServerStatus === STATUS.ERROR) {
      updateElementStatus('statusNotification', STATUS.ERROR);
    } else if (statusData.managerStatus === STATUS.WARN || isSafeMode ||
      statusData.tServerStatus === STATUS.WARN ||
      statusData.gcStatus === STATUS.WARN ||
      statusData.sServerStatus === STATUS.WARN) {
      updateElementStatus('statusNotification', STATUS.WARN);
    }
  };

  applyStatuses(getManagerGoalStateFromSession());

  getManager().always(function () {
    applyStatuses(getManagerGoalStateFromSession());
  });
}

/**
 * Updates the scan server notification based on REST v2 metrics status.
 */
function refreshSserverStatus() {
  return $.when(
    $.getJSON(REST_V2_PREFIX + '/sservers/summary'),
    $.getJSON(REST_V2_PREFIX + '/problems')
  ).done(function (summaryResp, problemsResp) {
    var summary = summaryResp && summaryResp[0] ? summaryResp[0] : {};
    var problems = problemsResp && problemsResp[0] ? problemsResp[0] : [];

    var hasSservers = summary && Object.keys(summary).length > 0;
    var hasProblemSserver = problems.some(function (problem) {
      return problem.type === 'SCAN_SERVER' || problem.serverType === 'SCAN_SERVER';
    });

    // 1) Display WARN when problem entries show any scan servers
    if (hasProblemSserver) {
      sessionStorage.sServerStatus = STATUS.WARN;
      updateElementStatus('sserverStatusNotification', STATUS.WARN);
      // 2) Display OK when there are no scan servers reported
    } else if (!hasSservers) {
      sessionStorage.sServerStatus = STATUS.OK;
      updateElementStatus('sserverStatusNotification', STATUS.OK);
      // 3) Display OK when scan servers exist and no problems
    } else {
      sessionStorage.sServerStatus = STATUS.OK;
      updateElementStatus('sserverStatusNotification', STATUS.OK);
    }
  }).fail(function () {
    // else, display ERROR when call fails
    sessionStorage.sServerStatus = STATUS.ERROR;
    updateElementStatus('sserverStatusNotification', STATUS.ERROR);
  });
}

/**
 * Creates the initial sidebar
 */
$(function () {
  refreshSidebar();
});

/**
 * Makes the REST call for the server status, generates the sidebar with the new information
 */
function refreshSidebar() {
  $.when(getStatus(), refreshSserverStatus()).always(function () {
    refreshSideBarNotifications();
  });
}

/**
 * Used to redraw the navbar
 */
function refreshNavBar() {
  refreshSidebar();
}

/**
 * Generates the sidebar notifications for servers
 */
function refreshSideBarNotifications() {

  const statusData = sessionStorage?.status ? JSON.parse(sessionStorage.status) : undefined;
  if (!statusData) {
    return;
  }
  statusData.sServerStatus = sessionStorage.sServerStatus || STATUS.OK;

  updateServerNotifications(statusData);
}
