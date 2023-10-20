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
  getManager().then(function () {

    // gather information about the manager
    const managerData = JSON.parse(sessionStorage.manager);
    const managerState = managerData.managerState;
    const managerGoalState = managerData.managerGoalState;

    const isSafeMode = managerState === 'SAFE_MODE' || managerGoalState === 'SAFE_MODE';
    const isCleanStop = managerState === 'CLEAN_STOP' || managerGoalState === 'CLEAN_STOP';

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

    // Setting overall servers status notification
    if ((statusData.managerStatus === STATUS.OK && !isSafeMode && !isCleanStop) &&
      statusData.tServerStatus === STATUS.OK &&
      statusData.gcStatus === STATUS.OK) {
      updateElementStatus('statusNotification', STATUS.OK);
    } else if (statusData.managerStatus === STATUS.ERROR || isCleanStop ||
      statusData.tServerStatus === STATUS.ERROR ||
      statusData.gcStatus === STATUS.ERROR) {
      updateElementStatus('statusNotification', STATUS.ERROR);
    } else if (statusData.managerStatus === STATUS.WARN || isSafeMode ||
      statusData.tServerStatus === STATUS.WARN ||
      statusData.gcStatus === STATUS.WARN) {
      updateElementStatus('statusNotification', STATUS.WARN);
    }

  });
}

/**
 * Updates the notification color for the recent logs icon within the debug dropdown
 */
function updateRecentLogsNotification(statusData) {
  if (statusData.logNumber > 0) {
    if (statusData.logsHaveError) {
      updateElementStatus('recentLogsNotifications', STATUS.ERROR);
    } else {
      updateElementStatus('recentLogsNotifications', STATUS.WARN);
    }
  } else {
    updateElementStatus('recentLogsNotifications', STATUS.OK);
  }
  // Number
  const logNumber = statusData.logNumber > 99 ? '99+' : statusData.logNumber;
  $('#recentLogsNotifications').html(logNumber);
}

/**
 * Updates the notification color for the table problems icon within the debug dropdown
 */
function updateTableProblemsNotification(statusData) {
  if (statusData.problemNumber > 0) {
    updateElementStatus('tableProblemsNotifications', STATUS.ERROR);
  } else {
    updateElementStatus('tableProblemsNotifications', STATUS.OK);
  }
  // Number
  var problemNumber = statusData.problemNumber > 99 ? '99+' : statusData.problemNumber;
  $('#tableProblemsNotifications').html(problemNumber);
}

/**
 * Updates the notification color for the debug dropdown icon
 */
function updateDebugDropdownNotification(statusData) {
  if (statusData.logNumber > 0 || statusData.problemNumber > 0) {
    if (statusData.logsHaveError || statusData.problemNumber > 0) {
      updateElementStatus('errorsNotification', STATUS.ERROR);
    } else {
      updateElementStatus('errorsNotification', STATUS.WARN);
    }
  } else {
    updateElementStatus('errorsNotification', STATUS.OK);
  }
  // Number
  var totalNumber = statusData.logNumber + statusData.problemNumber > 99 ?
    '99+' : statusData.logNumber + statusData.problemNumber;
  $('#errorsNotification').html(totalNumber);
}

/**
 * Creates the initial sidebar
 */
$(document).ready(function () {
  refreshSidebar();
});

/**
 * Makes the REST call for the server status, generates the sidebar with the new information
 */
function refreshSidebar() {
  getStatus().then(function () {
    refreshSideBarNotifications();
  });
}

/**
 * Used to redraw the navbar
 */
function refreshNavBar() {
  refreshSidebar();
  updateSystemAlerts();
}

/**
 * Generates the sidebar notifications for servers and logs
 */
function refreshSideBarNotifications() {

  const statusData = sessionStorage?.status ? JSON.parse(sessionStorage.status) : undefined;

  updateServerNotifications(statusData);
  updateRecentLogsNotification(statusData);
  updateTableProblemsNotification(statusData);
  updateDebugDropdownNotification(statusData);
}
