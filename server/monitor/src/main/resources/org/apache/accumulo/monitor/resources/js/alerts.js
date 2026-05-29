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

const alertHtmlTable = '#alertsTable';

var dataTableRef;
var categories;

function fetchTableData() {

  var highSwitchId = "alert-pri-switch-High";
  var savedValue = localStorage.getItem(highSwitchId + "-state");
  var high = false;
  if (savedValue === null || savedValue === 'true') {
    high = true;
  }

  var infoSwitchId = "alert-pri-switch-Info";
  savedValue = localStorage.getItem(infoSwitchId + "-state");
  var info = false;
  if (savedValue === null || savedValue === 'true') {
    info = true;
  }

  var categories = getStoredArray(ALERT_CATEGORIES);
  if (categories.length === 0) {
    sessionStorage.setItem(ALERTS, JSON.stringify([]));
    return $.Deferred().resolve().promise();
  }

  var cats = [];
  $.each(categories, function (index, cat) {
    var savedValue = localStorage.getItem("alert-cat-switch-" + cat + "-state");
    if (savedValue === null || savedValue === 'true') {
      cats.push(cat);
    }
  });
  return getAlerts(high, info, cats);
}

function getTableData() {
  return getStoredArray(ALERTS);
}

function loadAlertsPageData() {

  categories = getStoredArray(ALERT_CATEGORIES);
  if (categories.length === 0) {
    return getAlertCategories().then(function () {
      return fetchTableData();
    });
  } else {
    return fetchTableData();
  }
}

function refresh() {
  return loadAlertsPageData().then(function () {
    if (dataTableRef) {
      ajaxReloadTable(dataTableRef);
    }
  });
}

function createDataTable() {
  dataTableRef = $(alertHtmlTable).DataTable({
    "autoWidth": false,
    "ajax": function (data, callback) {
      callback({
        data: getTableData()
      });
    },
    "stateSave": true,
    "colReorder": true,
    "columnDefs": [{
      targets: '_all',
      defaultContent: '&mdash;'
    }],
    "columns": [{
        "data": "priority"
      },
      {
        "data": "category"
      },
      {
        "data": "message"
      }
    ]
  });
}

$(function () {
  loadAlertsPageData().then(function () {
    createDataTable();
  });
});
