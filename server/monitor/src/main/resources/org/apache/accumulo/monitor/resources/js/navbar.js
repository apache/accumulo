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
const SERVER_STATUS = {
  WARN: 'WARN',
  OK: 'OK',
  ERROR: 'ERROR'
};

/**
 * The class names of bootstrap notification color classes
 */
const COLOR_CLASS = {
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

  // Remove existing status classes
  $element.removeClass(Object.values(COLOR_CLASS).join(' '));

  switch (status) {
  case SERVER_STATUS.ERROR:
    $element.addClass(COLOR_CLASS.ERROR);
    break;
  case SERVER_STATUS.WARN:
    $element.addClass(COLOR_CLASS.WARNING);
    break;
  case SERVER_STATUS.OK:
    $element.addClass(COLOR_CLASS.NORMAL);
    break;
  default:
    console.error('Unrecognized status: ' + status);
  }
}

function updateIconDisplay(isSpecialIcon) {
  if (isSpecialIcon) {
    $('#managerStatusDot').hide();
    $('#managerStatusCone').show();
  } else {
    $('#managerStatusDot').show();
    $('#managerStatusCone').hide();
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

    // Show cone icon when the manager state is not normal, otherwise show dot icon
    updateIconDisplay(isSafeMode || isCleanStop);

    // setting manager status notification
    if (statusData.managerStatus === SERVER_STATUS.ERROR || isCleanStop) {
      updateElementStatus('managerStatusCone', SERVER_STATUS.ERROR);
      updateElementStatus('managerStatusDot', SERVER_STATUS.ERROR);
    } else if (statusData.managerStatus === SERVER_STATUS.WARN || isSafeMode) {
      updateElementStatus('managerStatusCone', SERVER_STATUS.WARN);
      updateElementStatus('managerStatusDot', SERVER_STATUS.WARN);
    } else if (statusData.managerStatus === SERVER_STATUS.OK) {
      updateElementStatus('managerStatusCone', SERVER_STATUS.OK);
      updateElementStatus('managerStatusDot', SERVER_STATUS.OK);
    } else {
      console.error('Unrecognized manager state: ' + statusData.managerStatus + '. Could not properly set manager status notification.');
    }

    // setting tserver status notification
    if (statusData.tServerStatus === SERVER_STATUS.OK) {
      updateElementStatus('serverStatusNotification', SERVER_STATUS.OK);
    } else if (statusData.tServerStatus === SERVER_STATUS.WARN) {
      updateElementStatus('serverStatusNotification', SERVER_STATUS.WARN);
    } else {
      updateElementStatus('serverStatusNotification', SERVER_STATUS.ERROR);
    }

    // setting gc status notification
    if (statusData.gcStatus === SERVER_STATUS.OK) {
      updateElementStatus('gcStatusNotification', SERVER_STATUS.OK);
    } else {
      updateElementStatus('gcStatusNotification', SERVER_STATUS.ERROR);
    }

    // Setting overall servers status notification
    if ((statusData.managerStatus === SERVER_STATUS.OK && !isSafeMode && !isCleanStop) &&
      statusData.tServerStatus === SERVER_STATUS.OK &&
      statusData.gcStatus === SERVER_STATUS.OK) {
      updateElementStatus('statusNotification', SERVER_STATUS.OK);
    } else if (statusData.managerStatus === SERVER_STATUS.ERROR || isCleanStop ||
      statusData.tServerStatus === SERVER_STATUS.ERROR ||
      statusData.gcStatus === SERVER_STATUS.ERROR) {
      updateElementStatus('statusNotification', SERVER_STATUS.ERROR);
    } else if (statusData.managerStatus === SERVER_STATUS.WARN || isSafeMode ||
      statusData.tServerStatus === SERVER_STATUS.WARN ||
      statusData.gcStatus === SERVER_STATUS.WARN) {
      updateElementStatus('statusNotification', SERVER_STATUS.WARN);
    }

  });
}

/**
 * Updates the notification color for the recent logs icon within the debug dropdown
 */
function updateRecentLogsNotification(statusData) {
  if (statusData.logNumber > 0) {
    if (statusData.logsHaveError) {
      updateElementStatus('recentLogsNotifications', SERVER_STATUS.ERROR);
    } else {
      updateElementStatus('recentLogsNotifications', SERVER_STATUS.WARN);
    }
  } else {
    updateElementStatus('recentLogsNotifications', SERVER_STATUS.OK);
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
    updateElementStatus('tableProblemsNotifications', SERVER_STATUS.ERROR);
  } else {
    updateElementStatus('tableProblemsNotifications', SERVER_STATUS.OK);
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
      updateElementStatus('errorsNotification', SERVER_STATUS.ERROR);
    } else {
      updateElementStatus('errorsNotification', SERVER_STATUS.WARN);
    }
  } else {
    updateElementStatus('errorsNotification', SERVER_STATUS.OK);
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
