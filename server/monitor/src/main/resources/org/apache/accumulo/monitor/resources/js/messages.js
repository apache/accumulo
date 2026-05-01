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

const messageCategoryList = '#message-category-list';
const messagePriorityList = '#message-priority-list';
const messageHtmlTable = '#messagesTable';
const messagePriorities = ['High', 'Info'];

var dataTableRef;

/**
 * Updates the list of message priorities
 */
function updateMessagePriorities() {
  var priorityList = $(messagePriorityList);
  $.each(messagePriorities, function (index, pri) {
    var switchId = "msg-pri-switch-" + pri;
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
      var div = $(document.createElement("div"));
      div.addClass("form-check form-switch");

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
        localStorage.setItem("msg-pri-switch-" + pri + "-state", $(this).is(':checked'));
        refresh();
      });
      div.append(input);

      var label = $(document.createElement("label"));
      label.addClass("form-check-label fs-6");
      label.attr("for", switchId);
      label.text(pri);
      div.append(label);

      priorityList.append(div);
    }
  });

}

/**
 * Updates the list of message categories
 */
function updateMessageCategories() {

  var categories = JSON.parse(sessionStorage[MESSAGE_CATEGORIES]);
  if (!Array.isArray(categories)) {
    categories = [];
  }

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
      var div = $(document.createElement("div"));
      div.addClass("form-check form-check-inline form-switch");

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
        localStorage.setItem("msg-cat-switch-" + cat + "-state", $(this).is(':checked'));
        refresh();
      });
      div.append(input);

      var label = $(document.createElement("label"));
      label.addClass("form-check-label fs-6");
      label.attr("for", switchId);
      label.text(cat);
      div.append(label);

      categoryList.append(div);
    }
  });

}

function fetchTableData() {

  var highSwitchId = "msg-pri-switch-High";
  var savedValue = localStorage.getItem(highSwitchId + "-state");
  var high = false;
  if (savedValue === null || savedValue === 'true') {
    high = true;
  }

  var InfoSwitchId = "msg-pri-switch-Info";
  savedValue = localStorage.getItem(InfoSwitchId + "-state");
  var info = false;
  if (savedValue === null || savedValue === 'true') {
    info = true;
  }

  var categories = JSON.parse(sessionStorage[MESSAGE_CATEGORIES]);
  if (!Array.isArray(categories)) {
    categories = [];
  }
  if (categories.length !== 0) {
    var cats = [];
    $.each(categories, function (index, cat) {
      var savedValue = localStorage.getItem("msg-cat-switch-" + cat + "-state");
      if (savedValue === null || savedValue === 'true') {
        cats.push(cat);
      }
    });
    getMessages(high, info, cats);
  }
}

function getTableData() {
  if (!sessionStorage[MESSAGES]) {
    return [];
  }
  return JSON.parse(sessionStorage[MESSAGES]);
}

function refresh() {
  $.when(getMessageCategories(), fetchTableData()).then(function () {
    updateMessagePriorities();
    updateMessageCategories();
    if (dataTableRef) {
      ajaxReloadTable(dataTableRef);
    }
  });
}

function createDataTable() {
  dataTableRef = $(messageHtmlTable).DataTable({
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
      defaultContent: '-'
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
  $.when(getMessageCategories(), fetchTableData()).then(function () {
    updateMessagePriorities();
    updateMessageCategories();
    createDataTable();
  });
});
