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
 * Add new system-wide messages to this object.
 */
const alertMap = {
  /**
   * @property {string} message - The message to display within the alert banner.
   * @property {string} id - Short string used to identify the message.
   * @property {string} alertClass - The bootstrap class for which alert style to apply.
   * @property {string} notificationClass - The color class to apply to the status notification in the navbar.
   */
  CLEAN_STOP: {
    message: "The manager state is CLEAN_STOP.",
    id: "cleanStop",
    alertClass: "alert-danger",
    notificationClass: "error-inv"
  },
  SAFE_MODE: {
    message: "The manager state is SAFE_MODE.",
    id: "safeMode",
    alertClass: "alert-warning",
    notificationClass: "warning-inv"
  }
};

/**
 * Idempotently append an alert message
 */
function appendAlertMessage(alertInfo) {
  if (alertInfo && $('#' + alertInfo.id).length === 0) {
    const li = $('<li></li>').text(alertInfo.message).attr('id', alertInfo.id);
    $("#alertMessages").append(li);
  }
}

function removeAlertMessage(alertInfo) {
  if (alertInfo) {
    $('#' + alertInfo.id).remove();
  }
}

/**
 * Set the class for system alert.
 * 
 * @param {string} alertClass - Class to be applied to system alert.
 */
function setAlertClass(alertClass) {
  $("#systemAlert").removeClass("alert-warning alert-danger").addClass(alertClass);
}

/**
 * Set the class for notification status.
 * 
 * @param {string} notificationClass - Class to be applied to notification status.
 */
function setNotificationClass(notificationClass) {
  $("#alertStatus").removeClass("warning error").addClass(notificationClass);
}

function updateAlertVisibility() {
  if ($("#alertMessages").children().length === 0) {
    $("#systemAlert").hide();
    $("#alertStatus").hide();
  } else if (localStorage.getItem('alertDismissed') === 'true') {
    $("#systemAlert").hide();
  } else {
    $("#systemAlert").show();
  }
}

/**
 * Appends or removes an alert message based on the provided parameters.
 * 
 * @param {string} alertType - Type of alert (e.g., 'CLEAN_STOP', 'SAFE_MODE').
 * @param {string} operation - Operation to perform ('apply' or 'remove').
 */
function handleAlert(alertType, operation) {
  const alertInfo = alertMap[alertType];
  switch (operation) {
  case 'apply':
    appendAlertMessage(alertInfo);
    setAlertClass(alertInfo.alertClass);
    setNotificationClass(alertInfo.notificationClass);
    $("#alertStatus").show();
    break;
  case 'remove':
    removeAlertMessage(alertInfo);
    break;
  default:
    console.error('Alert operation not recognized: ' + operation);
    return;
  }

  updateAlertVisibility();
}

function updateManagerAlerts() {
  getManager().then(function () {

    // gather information about the manager
    const managerData = JSON.parse(sessionStorage.manager);
    const managerState = managerData.managerState;
    const managerGoalState = managerData.managerGoalState;

    const isSafeMode = managerState === 'SAFE_MODE' || managerGoalState === 'SAFE_MODE';
    const isCleanStop = managerState === 'CLEAN_STOP' || managerGoalState === 'CLEAN_STOP';

    const currentState = isCleanStop ? 'CLEAN_STOP' : isSafeMode ? 'SAFE_MODE' : null;
    const prevStoredState = localStorage.getItem('currentState');

    if (currentState !== prevStoredState) {
      localStorage.removeItem('alertDismissed');
      localStorage.setItem('currentState', currentState);
    }

    if (isCleanStop) {
      handleAlert('SAFE_MODE', 'remove');
      handleAlert('CLEAN_STOP', 'apply');
    } else if (isSafeMode) {
      handleAlert('CLEAN_STOP', 'remove');
      handleAlert('SAFE_MODE', 'apply');
    } else {
      handleAlert('CLEAN_STOP', 'remove');
      handleAlert('SAFE_MODE', 'remove');
    }
  }).catch(function () {
    console.error('Failed to retrieve manager data');
  })
}

/**
 * Pulls the neccesarry data then refreshes the system wide alert banner and status notification
 */
function updateSystemAlerts() {
  updateManagerAlerts();
}

$(document).ready(function () {

  // dismiss the alert when clicked
  $('#systemAlertCloseButton').click(function () {
    $("#systemAlert").hide();
    localStorage.setItem('alertDismissed', 'true');
  });

  // when clicked, the status icon will bring the alert back up
  $('#alertStatus').click(function () {
    $("#systemAlert").show();
    localStorage.removeItem('alertDismissed');
  });

  updateSystemAlerts();
});
