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
const messageHtmlTable = '#messagesTable';

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
        switchElement.prop('checked', true);
      } else {
        switchElement.prop('checked', false);
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
        localStorage.setItem("msg-cat-switch-" + cat + "-state", $(this).is(':checked'));
        refreshMessages();
      });
      div.append(input);

      var label = $(document.createElement("label"));
      label.addClass("form-check-label");
      label.attr("for", switchId);
      label.text(cat);
      div.append(label);

      categoryList.append(div);
    }
  });

}

function refreshMessages() {

  var htmlTable = $(messageHtmlTable)
  $.when(getMessageCategories(), getMessages()).then(function () {

    var categories = JSON.parse(sessionStorage[MESSAGE_CATEGORIES]);
    if (!Array.isArray(categories)) {
      categories = [];
    }
    if (categories.length === 0) {
      $(htmlTable).hide();
    } else {
      var messages = JSON.parse(sessionStorage[MESSAGES]);
      var body = $(document.createElement('tbody'));
      var rowsCreated = false;
      $.each(categories, function (index, cat) {
        var savedValue = localStorage.getItem("msg-cat-switch-" + cat + "-state");
        if (savedValue === null || savedValue === 'true') {
          var catMessages = messages[cat];
          if (Array.isArray(catMessages)) {
            $(htmlTable).find('tbody').remove();
            $.each(catMessages, function (index, msg) {
              var row = $(document.createElement("tr"));
              var catCol = $(document.createElement("td"));
              catCol.text(cat);
              row.append(catCol);
              var msgCol = $(document.createElement("td"));
              msgCol.text(msg);
              row.append(msgCol);
              body.append(row);
              rowsCreated = true;
            });
          }
        }
      });
      if (rowsCreated === true) {
        htmlTable.append(body);
        $(htmlTable).show();
      } else {
        $(htmlTable).hide();
      }
    }

  });
}

function refresh() {
  $.when(getMessageCategories(), getMessages()).then(function () {
    updateMessageCategories();
    refreshMessages();
  });
}

$(function () {
  refresh();
});
