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
 * Used to redraw the navbar
 */
function refreshNavBar() {
  refreshSidebar();
}

/**
 * Generates the sidebar notifications for servers and logs
 */
function refreshSidebar() {
  getStatus().then(function () {
    const statusData = sessionStorage?.status ? JSON.parse(sessionStorage.status) : undefined;

    updateStatusNotifications(statusData);

    // Setting "Recent Logs" notifications
    // Color
    if (statusData.logNumber > 0) {
      if (statusData.logsHaveError) {
        $('#recentLogsNotifications').removeClass('warning').removeClass('normal').addClass('error');
      } else {
        $('#recentLogsNotifications').removeClass('error').removeClass('normal').addClass('warning');
      }
    } else {
      $('#recentLogsNotifications').removeClass('error').removeClass('warning').addClass('normal');
    }
    // Number
    var logNumber = statusData.logNumber > 99 ? '99+' : statusData.logNumber;
    $('#recentLogsNotifications').html(logNumber);


    // Setting "Table Problems" notifications
    // Color
    if (statusData.problemNumber > 0) {
      $('#tableProblemsNotifications').removeClass('normal').addClass('error');
    } else {
      $('#tableProblemsNotifications').removeClass('error').addClass('normal');
    }
    // Number
    var problemNumber = statusData.problemNumber > 99 ? '99+' : statusData.problemNumber;
    $('#tableProblemsNotifications').html(problemNumber);


    // Setting "Debug" overall logs notifications
    // Color
    if (statusData.logNumber > 0 || statusData.problemNumber > 0) {
      if (statusData.logsHaveError || statusData.problemNumber > 0) {
        $('#errorsNotification').removeClass('warning').removeClass('normal').addClass('error');
      } else {
        $('#errorsNotification').removeClass('error').removeClass('normal').addClass('warning');
      }
    } else {
      $('#errorsNotification').removeClass('error').removeClass('warning').addClass('normal');
    }
    // Number
    var totalNumber = statusData.logNumber + statusData.problemNumber > 99 ?
      '99+' : statusData.logNumber + statusData.problemNumber;
    $('#errorsNotification').html(totalNumber);
  });
}

/**
 * Set the individual status notifications
 */
function updateStatusNotifications(statusData) {

  // manager
  getManager().then(function () {
    const managerData = JSON.parse(sessionStorage.manager);
    const managerState = managerData.managerState;
    const managerGoalState = managerData.managerGoalState;

    const isSafeMode = managerState === 'SAFE_MODE' || managerGoalState === 'SAFE_MODE';
    const isCleanStop = managerState === 'CLEAN_STOP' || managerGoalState === 'CLEAN_STOP';

    if (statusData.managerStatus === 'ERROR' || isCleanStop) {
      $('#managerStatusNotification').removeClass('normal').removeClass('warning').addClass('error');
    } else if (statusData.managerStatus !== 'OK' || isSafeMode) {
      $('#managerStatusNotification').removeClass('normal').removeClass('error').addClass('warning');
    } else {
      $('#managerStatusNotification').removeClass('error').removeClass('warning').addClass('normal');
    }
  });

  // tserver
  if (statusData.tServerStatus === 'OK') {
    $('#serverStatusNotification').removeClass('error').removeClass('warning').addClass('normal');
  } else if (statusData.tServerStatus === 'WARN') {
    $('#serverStatusNotification').removeClass('error').removeClass('normal').addClass('warning');
  } else {
    $('#serverStatusNotification').removeClass('normal').removeClass('warning').addClass('error');
  }

  // GC
  if (statusData.gcStatus === 'OK') {
    $('#gcStatusNotification').removeClass('error').addClass('normal');
  } else {
    $('#gcStatusNotification').addClass('error').removeClass('normal');
  }
}
