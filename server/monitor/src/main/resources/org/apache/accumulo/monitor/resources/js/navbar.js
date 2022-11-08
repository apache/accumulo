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
 * Creates the initial sidebar
 */
$(document).ready(function () {
  refreshSidebar();
});

/**
 * Makes the REST calls, generates the sidebar with the new information
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

  var data = sessionStorage.status === undefined ?
    undefined : JSON.parse(sessionStorage.status);

  // Setting individual status notification
  if (data.managerStatus === 'OK') {
    $('#managerStatusNotification').removeClass('error').addClass('normal');
  } else {
    $('#managerStatusNotification').removeClass('normal').addClass('error');
  }
  if (data.tServerStatus === 'OK') {
    $('#serverStatusNotification').removeClass('error').removeClass('warning').
    addClass('normal');
  } else if (data.tServerStatus === 'WARN') {
    $('#serverStatusNotification').removeClass('error').removeClass('normal').
    addClass('warning');
  } else {
    $('#serverStatusNotification').removeClass('normal').removeClass('warning').
    addClass('error');
  }
  if (data.gcStatus === 'OK') {
    $('#gcStatusNotification').removeClass('error').addClass('normal');
  } else {
    $('#gcStatusNotification').addClass('error').removeClass('normal');
  }

  // Setting overall status notification
  if (data.managerStatus === 'OK' &&
    data.tServerStatus === 'OK' &&
    data.gcStatus === 'OK') {
    $('#statusNotification').removeClass('error').removeClass('warning').
    addClass('normal');
  } else if (data.managerStatus === 'ERROR' ||
    data.tServerStatus === 'ERROR' ||
    data.gcStatus === 'ERROR') {
    $('#statusNotification').removeClass('normal').removeClass('warning').
    addClass('error');
  } else if (data.tServerStatus === 'WARN') {
    $('#statusNotification').removeClass('normal').removeClass('error').
    addClass('warning');
  }

  // Setting "Recent Logs" notifications
  // Color
  if (data.logNumber > 0) {
    if (data.logsHaveError) {
      $('#recentLogsNotifications').removeClass('warning').removeClass('normal').addClass('error');
    } else {
      $('#recentLogsNotifications').removeClass('error').removeClass('normal').addClass('warning');
    }
  } else {
    $('#recentLogsNotifications').removeClass('error').removeClass('warning').addClass('normal');
  }
  // Number
  var logNumber = data.logNumber > 99 ? '99+' : data.logNumber;
  $('#recentLogsNotifications').html(logNumber);


  // Setting "Table Problems" notifications
  // Color
  if (data.problemNumber > 0) {
    $('#tableProblemsNotifications').removeClass('normal').addClass('error');
  } else {
    $('#tableProblemsNotifications').removeClass('error').addClass('normal');
  }
  // Number
  var problemNumber = data.problemNumber > 99 ? '99+' : data.problemNumber;
  $('#tableProblemsNotifications').html(problemNumber);


  // Setting "Debug" overall logs notifications
  // Color
  if (data.logNumber > 0 || data.problemNumber > 0) {
    if (data.logsHaveError || data.problemNumber > 0) {
      $('#errorsNotification').removeClass('warning').removeClass('normal').addClass('error');
    } else {
      $('#errorsNotification').removeClass('error').removeClass('normal').addClass('warning');
    }
  } else {
    $('#errorsNotification').removeClass('error').removeClass('warning').addClass('normal');
  }
  // Number
  var totalNumber = data.logNumber + data.problemNumber > 99 ?
    '99+' : data.logNumber + data.problemNumber;
  $('#errorsNotification').html(totalNumber);
}
