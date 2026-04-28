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

const suggestionCategoryList = '#suggestion-category-list';

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
  const managerGoalState = statusData.managerGoalState;
  const isSafeMode = managerGoalState === 'SAFE_MODE';
  const isCleanStop = managerGoalState === 'CLEAN_STOP';
  const componentStatuses = [
    getComponentStatus(statusData, 'MANAGER'),
    getComponentStatus(statusData, 'TABLET_SERVER'),
    getComponentStatus(statusData, 'GARBAGE_COLLECTOR'),
    getComponentStatus(statusData, 'SCAN_SERVER'),
    getComponentStatus(statusData, 'COMPACTOR')
  ];
  const managerStatus = componentStatuses[0];

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

  updateElementStatus('serverStatusNotification', componentStatuses[1]);
  updateElementStatus('gcStatusNotification', componentStatuses[2]);
  updateElementStatus('sserverStatusNotification', componentStatuses[3]);
  updateElementStatus('compactorStatusNotification', componentStatuses[4]);

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
  refreshSidebar();
});

/**
 * Makes the REST call for the server status, generates the sidebar with the new information
 */
function refreshSidebar() {
  getStatus().always(function () {
    refreshSideBarNotifications();
  });
  getSuggestionCategories().then(function () {
    updateSuggestionCategories();
  });
}

/**
 * Updates the list of suggestion categories
 */
function updateSuggestionCategories() {

  var categories = JSON.parse(sessionStorage[SUGGESTION_CATEGORIES]);
  if (!Array.isArray(categories)) {
    categories = [];
  }

  var categoryList = $(suggestionCategoryList);
  $.each(categories, function (index, cat) {

    var switchId = "sug-cat-switch-" + cat;
    var switchElement = "#" + switchId;
    var savedValue = localStorage.getItem(switchId + "-state");

    if ($(switchElement).length) {
      // update it
      if (savedValue === null || savedValue === 'true') {
        switchElement.prop('checked', true);
      } else {
        switchElement.prop('checked', false);
      }
    } else {
      // create it
      var div = $(document.createElement("div"));
      div.addClass("form-check form-check-reverse form-switch");

      var input = $(document.createElement("input"));
      input.addClass("form-check-input");
      input.attr("type", "checkbox");
      input.attr("role", "switch");
      input.attr("id", switchId);

      if (savedValue === null || savedValue === 'true') {
        input.prop('checked', true);
      } else {
        input.prop('checked', false);
      }

      input.on("change", function () {
        localStorage.setItem("sug-cat-switch-" + cat + "-state", $(this).is(':checked'));
      });
      div.append(input);

      var label = $(document.createElement("label"));
      label.addClass("form-check-label");
      label.attr("for", switchId);
      label.text("Suggestions: " + cat);
      div.append(label);

      categoryList.append(div);
    }
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
