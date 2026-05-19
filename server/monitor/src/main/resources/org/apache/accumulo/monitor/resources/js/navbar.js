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

var categories;

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
  updateMessagePriorities();
  refreshSidebar();

  categories = getStoredArray(MESSAGE_CATEGORIES);
  if (categories.length === 0) {
    getMessageCategories().then(function () {
      categories = getStoredArray(MESSAGE_CATEGORIES);
      updateMessageCategories();
    });
  } else {
    updateMessageCategories();
  }
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
 * Returns the effective dark theme preference.
 */
function isDarkThemeEnabled() {
  var storedValue = localStorage.getItem("dark-theme-enabled");
  if (storedValue === null) {
    return window.matchMedia('(prefers-color-scheme: dark)').matches;
  }
  return storedValue === 'true';
}

/**
 * Set the theme based on the user preferences
 */
function setTheme() {
  if (isDarkThemeEnabled() === true) {
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

  if (isDarkThemeEnabled() === true) {
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

/**
 * Update the High and Info Message Category Switches
 */
function updateMessagePriorities() {
  var messagePriorities = ['High', 'Info'];
  $.each(messagePriorities, function (index, pri) {
    var switchId = "msg-pri-switch-" + pri;
    var switchElement = "#" + switchId;
    var savedValue = localStorage.getItem(switchId + "-state");

    // update it
    if (savedValue === null || savedValue === 'true') {
      $(switchElement).prop('checked', true);
    } else {
      $(switchElement).prop('checked', false);
    }

    $(switchElement).on("change", function () {
      localStorage.setItem("msg-pri-switch-" + pri + "-state", $(this).is(':checked'));
      if (window.location.pathname.endsWith('/messages')) {
        refresh();
      }
    });
  });
}

/**
 * Update the High and Info Message Category Switches
 */
function updateMessageCategories() {
  const messageCategoryList = '#categories-list';

  var categoryList = $(messageCategoryList);
  $.each(categories, function (index, cat) {

    var switchId = "msg-cat-switch-" + cat;
    var switchElement = "#" + switchId;
    var savedValue = localStorage.getItem(switchId + "-state");

    if ($(switchElement).length) {
      // update it
      if (savedValue === null || savedValue === 'true') {
        $(switchElement).prop('checked', true);
      } else {
        $(switchElement).prop('checked', false);
      }
    } else {
      // create it
      var li = $(document.createElement("li"));

      var outerDiv = $(document.createElement("div"));
      outerDiv.addClass("dropdown-item d-flex justify-content-between align-items-center small");

      var div = $(document.createElement("div"));
      div.addClass("form-check form-switch d-flex align-items-center mb-0 p-0 fs-6");

      var input = $(document.createElement("input"));
      input.addClass("form-check-input float-none m-0");
      input.attr("type", "checkbox");
      input.attr("role", "switch");
      input.attr("id", switchId);

      if (savedValue === null || savedValue === 'true') {
        input.prop('checked', true);
      } else {
        input.prop('checked', false);
      }

      input.on("change", function () {
        localStorage.setItem("msg-cat-switch-" + cat + "-state", $(this).is(':checked'));
        if (window.location.pathname.endsWith('/messages')) {
          refresh();
        }
      });
      div.append(input);

      var label = $(document.createElement("label"));
      label.addClass("form-check-label");
      label.attr("for", switchId);
      label.text(cat);

      outerDiv.append(label);
      outerDiv.append(div);
      li.append(outerDiv);
      categoryList.append(li);
    }
  });

}
