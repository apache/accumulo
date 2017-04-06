/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

/**
 * Creates the initial sidebar
 */
$(document).ready(function() {
  refreshSidebar();
});

/**
 * Makes the REST calls, generates the sidebar with the new information
 */
function refreshSidebar() {
  $.ajaxSetup({
    async: false
  });
  getStatus();
  $.ajaxSetup({
    async: true
  });
  refreshSideBarNotifications();
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

  $('#currentDate').html(Date());

  var data = sessionStorage.status === undefined ?
      undefined : JSON.parse(sessionStorage.status);

  // Setting individual status notification
  if (data.masterStatus == 'OK') {
    $('#masterStatusNotification').removeClass('error').addClass('normal');
  } else {
    $('#masterStatusNotification').removeClass('normal').addClass('error');
  }
  if (data.tServerStatus == 'OK') {
    $('#serverStatusNotification').removeClass('error').removeClass('warning').
        addClass('normal');
  } else if (data.tServerStatus == 'WARN') {
    $('#serverStatusNotification').removeClass('error').removeClass('normal').
        addClass('warning');
  } else {
    $('#serverStatusNotification').removeClass('normal').removeClass('warning').
        addClass('error');
  }
  if (data.gcStatus == 'OK') {
    $('#gcStatusNotification').removeClass('error').addClass('normal');
  } else {
    $('#gcStatusNotification').addClass('error').removeClass('normal');
  }

  // Setting overall status notification
  if (data.masterStatus == 'OK' &&
      data.tServerStatus == 'OK' &&
      data.gcStatus == 'OK') {
    $('#statusNotification').removeClass('error').removeClass('warning').
        addClass('normal');
  } else if (data.masterStatus == 'ERROR' ||
      data.tServerStatus == 'ERROR' ||
      data.gcStatus == 'ERROR') {
    $('#statusNotification').removeClass('normal').removeClass('warning').
        addClass('error');
  } else if (data.tServerStatus == 'WARN') {
    $('#statusNotification').removeClass('normal').removeClass('error').
        addClass('warning');
  }

  // Setting individual logs notifications
  // Color
  if (data.logNumber > 0) {
    if (data.logsHaveError) {
      $('#recentLogsNotifications').removeClass('warning').addClass('error');
    } else {
      $('#recentLogsNotifications').removeClass('error').addClass('warning');
    }
  } else {
    $('#recentLogsNotifications').removeClass('error').removeClass('warning');
  }
  // Number
  var logNumber = data.logNumber > 99 ? '99+' : data.logNumber;
  $('#recentLogsNotifications').html(logNumber);
  // Color
  if (data.problemNumber > 0) {
    $('#tableProblemsNotifications').addClass('error');
  } else {
    $('#tableProblemsNotifications').removeClass('error');
  }
  // Number
  var problemNumber = data.problemNumber > 99 ? '99+' : data.problemNumber;
  $('#tableProblemsNotifications').html(problemNumber);
  // Setting overall logs notifications
  // Color
  if (data.logNumber > 0 || data.problemNumber > 0) {
    if (data.logsHaveError || data.problemNumber > 0) {
      $('#errorsNotification').removeClass('warning').addClass('error');
    } else {
      $('#errorsNotification').removeClass('error').addClass('warning');
    }
  } else {
    $('#errorsNotification').removeClass('error').removeClass('warning');
  }

  // Number
  var totalNumber = data.logNumber + data.problemNumber > 99 ?
      '99+' : data.logNumber + data.problemNumber;
  $('#errorsNotification').html(totalNumber);
}
