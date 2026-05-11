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

const NAVBAR_COMPONENTS = [{
  statusKey: 'MANAGER',
  indicatorId: 'managerStatusNotification',
  countId: 'managerStatusCount'
}, {
  statusKey: 'TABLET_SERVER',
  indicatorId: 'serverStatusNotification',
  countId: 'serverStatusCount'
}, {
  statusKey: 'GARBAGE_COLLECTOR',
  indicatorId: 'gcStatusNotification',
  countId: 'gcStatusCount'
}, {
  statusKey: 'SCAN_SERVER',
  indicatorId: 'sserverStatusNotification',
  countId: 'sserverStatusCount'
}, {
  statusKey: 'COMPACTOR',
  indicatorId: 'compactorStatusNotification',
  countId: 'compactorStatusCount'
}];

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

function updateServerCount(elementId, status) {
  const $element = $(`#${elementId}`);

  if (!status || Number(status.serverCount) <= 0) {
    $element.text('0/0');
    return;
  }

  const total = Number(status.serverCount);
  const problem = Number(status.problemServerCount || 0);
  const responding = Math.max(0, total - problem);

  $element.text(`${responding}/${total}`);
}

/**
 * Updates the notifications of the servers dropdown notification as well as the individual server notifications.
 * @param {JSON} statusData object containing the status info for the servers
 */
function updateServerNotifications(statusData) {
  const managerGoalState = statusData.managerGoalState;
  const isSafeMode = managerGoalState === 'SAFE_MODE';
  const isCleanStop = managerGoalState === 'CLEAN_STOP';
  const componentStatuses = NAVBAR_COMPONENTS.map(function (component) {
    return getComponentStatus(statusData, component.statusKey);
  });
  const managerStatus = componentStatuses[0];
  const componentData = NAVBAR_COMPONENTS.map(function (component) {
    return statusData.componentStatuses?.[component.statusKey] || null;
  });

  // setting manager status notification
  if (managerStatus === STATUS.ERROR || isCleanStop) {
    updateElementStatus('managerStatusNotification', STATUS.ERROR);
  } else if (managerStatus === STATUS.WARN || isSafeMode) {
    updateElementStatus('managerStatusNotification', STATUS.WARN);
  } else if (managerStatus === STATUS.OK) {
    updateElementStatus('managerStatusNotification', STATUS.OK);
  } else {
    console.error('Unrecognized manager state: ' + managerStatus +
      '. Could not properly set manager status notification.');
  }

  NAVBAR_COMPONENTS.forEach(function (component, index) {
    updateServerCount(component.countId, componentData[index]);
    if (index === 0) {
      return;
    }
    updateElementStatus(component.indicatorId, componentStatuses[index]);
  });

  // Setting overall servers status notification
  if (!isSafeMode && !isCleanStop && componentStatuses.every(status => status === STATUS.OK)) {
    updateElementStatus('statusNotification', STATUS.OK);
  } else if (isCleanStop || componentStatuses.some(status => status === STATUS.ERROR)) {
    updateElementStatus('statusNotification', STATUS.ERROR);
  } else if (isSafeMode || componentStatuses.some(status => status === STATUS.WARN)) {
    updateElementStatus('statusNotification', STATUS.WARN);
  }
}

/**
 * Creates the initial sidebar
 */
$(function () {
  setTheme();
  updateDarkThemeSwitch();
  refreshSidebar();
});

/**
 * Makes the REST call for the server status, generates the sidebar with the new information
 */
function refreshSidebar() {
  getStatus().always(function () {
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

  updateServerNotifications(statusData);
}

/**
 * Set the theme based on the user
 * preferences
 */
function setTheme() {
  var setDarkMode = false;
  var storedValue = localStorage.getItem("dark-theme-enabled");
  if (storedValue === null) {
    setDarkMode = window.matchMedia('(prefers-color-scheme: dark)').matches;
  } else {
    setDarkMode = storedValue === 'true';
  }

  if (setDarkMode === true) {
    document.documentElement.setAttribute('data-bs-theme', 'dark');
  } else {
    document.documentElement.setAttribute('data-bs-theme', 'light');
  }
}

/**
 * Update the Dark Theme Switch in the Preference list
 */
function updateDarkThemeSwitch() {
  var storageKey = "dark-theme-enabled";
  var darkThemeSwitchElement = $('#darkThemeSwitch');
  var savedValue = localStorage.getItem(storageKey);

  if (savedValue === 'true') {
    darkThemeSwitchElement.prop('checked', true);
  } else {
    darkThemeSwitchElement.prop('checked', false);
  }

  darkThemeSwitchElement.on("change", function () {
    var enableDarkTheme = $(this).is(':checked');
    localStorage.setItem(storageKey, enableDarkTheme);
    document.documentElement.setAttribute('data-bs-theme', enableDarkTheme ? 'dark' : 'light');
  });
}
