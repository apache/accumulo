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
 * Add new system-wide messages to this object
 * 
 * message: the message to display within the alert banner
 * id: short string used to identify the message
 * alertClass: the bootstrap class for which alert style to apply
 * notificationClass: the color class to apply to the status notification in the navbar
 */
const alertMap = {
  CLEAN_STOP: {
    message: "The manager state is in CLEAN_STOP.",
    id: "cleanStop",
    alertClass: "alert-danger",
    notifcationClass: "error"
  },
  SAFE_MODE: {
    message: "The manager is in SAFE_MODE.",
    id: "safeMode",
    alertClass: "alert-warning",
    notifcationClass: "warning"
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

function setAlertClass(alertClass) {
  $("#systemAlert").removeClass("alert-warning alert-danger").addClass(alertClass);
}

function setNotificationClass(notifcationClass) {
  $("#alertStatus").removeClass("warning error").addClass(notifcationClass);
}

function showAlert() {
  $("#systemAlert").show();
  $("#alertStatus").show();
}

function hideAlert() {
  $("#systemAlert").hide();
  $("#alertStatus").hide();
}

/**
 * Appends or removes an alert message based on the provided parameters.
 *
 * @param {string} alertType - Specifies the type of alert. Should match keys in the alertMap object (e.g., 'CLEAN_STOP', 'SAFE_MODE').
 * @param {string} operation - Specifies the operation to perform on the alert. Accepted values are 'apply' to apply the alert or 'remove' to remove it.
 */
function handleAlert(alertType, operation) {
  const alertInfo = alertMap[alertType];
  if (operation === 'apply') {
    appendAlertMessage(alertInfo);
    setAlertClass(alertInfo.alertClass);
    setNotificationClass(alertInfo.notifcationClass);
    showAlert();
  } else if (operation === 'remove') {
    removeAlertMessage(alertInfo);
    // Check if there are any more list items left. If none, hide the alert.
    if ($("#alertMessages").children().length === 0) {
      hideAlert();
    }
  } else {
    console.error('Alert operation not recognized')
  }
}

function updateManagerAlerts() {
  getManager().then(function () {

    // gather information about the manager
    const managerData = JSON.parse(sessionStorage.manager);
    const managerState = managerData.managerState;
    const managerGoalState = managerData.managerGoalState;

    const isSafeMode = managerState === 'SAFE_MODE' || managerGoalState === 'SAFE_MODE';
    const isCleanStop = managerState === 'CLEAN_STOP' || managerGoalState === 'CLEAN_STOP';

    handleAlert('SAFE_MODE', 'remove');
    handleAlert('CLEAN_STOP', 'remove');

    if (isCleanStop) {
      handleAlert('CLEAN_STOP', 'apply');
    } else if (isSafeMode) {
      handleAlert('SAFE_MODE', 'apply');
    }
  })
}

/**
 * Pulls the neccesarry data then refreshes the system wide alert banner and status notification
 */
function updateSystemAlerts() {
  updateManagerAlerts();
}

$(document).ready(function () {
  updateSystemAlerts();
});
