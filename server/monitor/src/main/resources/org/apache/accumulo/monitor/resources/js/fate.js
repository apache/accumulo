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

const fateHtmlTable = '#fateTable';

var dataTableRef;

function getTableData() {
  return getStoredArray(FATE);
}

/**
 * Renders array as comma separated list or a dash if the list is empty
 */
function renderListOrDash(data, type) {
  if (Array.isArray(data)) {
    if (data.length === 0) {
      return type === 'display' ? '&mdash;' : '';
    }
    return data.join(', ');
  }
  if (data === null || data === undefined || data === '') {
    return type === 'display' ? '&mdash;' : '';
  }
  return data;
}

function createDataTable() {
  $(fateHtmlTable).find('thead').remove();
  $(fateHtmlTable).find('tbody').remove();
  dataTableRef = $(fateHtmlTable).DataTable({
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
        "data": "type",
        "title": "Type"
      },
      {
        "data": "op",
        "title": "Operation"
      },
      {
        "data": "id",
        "title": "Id"
      },
      {
        "data": "status",
        "title": "State"
      },
      {
        "data": "created",
        "title": "Created",
        "render": function (data, type, row) {
          if (type === 'display') data = dateFormat(data);
          return data;
        }
      },
      {
        "data": "created",
        "title": "Age",
        "render": function (data, type, row) {
          var dur = Date.now() - data;
          if (type === 'display') dur = timeDuration(dur);
          return dur;
        }
      },
      {
        "data": "heldLocks",
        "title": "Locks Held",
        "render": renderListOrDash
      },
      {
        "data": "waitingLocks",
        "title": "Locks Waiting On",
        "render": renderListOrDash
      },
      {
        "data": "lockRange",
        "title": "Lock Range Type"
      }
    ]
  });
}

function refresh() {
  return getFate().then(function () {
    if (dataTableRef) {
      ajaxReloadTable(dataTableRef);
    }
  });
}


$(function () {
  getFate().then(function () {
    createDataTable();
  });
});
