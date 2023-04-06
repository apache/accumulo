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

var tableID;
var problemSummaryTable;
var problemDetailTable;
$(document).ready(function () {
  // Create a table for summary. See datatables doc for more info on the dom property
  problemSummaryTable = $('#problemSummary').DataTable({
    "ajax": {
      "url": '/rest/problems/summary',
      "dataSrc": "problemSummary"
    },
    "stateSave": true,
    "dom": 't<"align-left"l>p',
    "columnDefs": [{
      "targets": "big-num",
      "render": function (data, type, row) {
        if (type === 'display') data = bigNumberForQuantity(data);
        return data;
      }
    }],
    "columns": [{
        "data": "tableName",
        "type": "html",
        "render": function (data, type, row, meta) {
          if (type === 'display') data = '<a href="/tables/' + row.tableID + '">' + row.tableName + '</a>';
          return data;
        }
      },
      {
        "data": "fileRead"
      },
      {
        "data": "fileWrite"
      },
      {
        "data": "tableLoad"
      },
      {
        "data": "tableID",
        "type": "html",
        "render": function (data, type, row, meta) {
          if (type === 'display') data = '<a href="javascript:clearTableProblemsTable(\'' +
            row.tableID + '\');">clear ALL problems with table ' + row.tableName + '</a>';
          return data;
        }
      }
    ]
  });
  // Create a table for details
  problemDetailTable = $('#problemDetails').DataTable({
    "ajax": {
      "url": '/rest/problems/details',
      "dataSrc": "problemDetails"
    },
    "stateSave": true,
    "dom": 't<"align-left"l>p',
    "columnDefs": [{
      "targets": "date",
      "render": function (data, type, row) {
        if (type === 'display') data = dateFormat(data);
        return data;
      }
    }],
    "columns": [{
        "data": "tableName",
        "type": "html",
        "render": function (data, type, row, meta) {
          if (type === 'display') data = '<a href="/tables/' + row.tableID + '">' + row.tableName + '</a>';
          return data;
        }
      },
      {
        "data": "type"
      },
      {
        "data": "server"
      },
      {
        "data": "time"
      },
      {
        "data": "resource"
      },
      {
        "data": "exception"
      },
      {
        "data": "tableID",
        "type": "html",
        "render": function (data, type, row, meta) {
          if (type === 'display') data = '<a href="javascript:clearDetailsProblemsTable(\'' +
            row.tableID + '\',\'' + row.resource + '\',\'' + row.type + '\')">clear this problem</a>';
          return data;
        }
      }
    ]
  });
});

/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshProblems() {
  refreshNavBar();
  refreshProblemSummaryTable();
  refreshProblemDetailsTable();
}

/**
 * Used to redraw the page
 */
function refresh() {
  refreshProblems();
}

/**
 * Makes REST POST call to clear the problem summary table
 *
 * @param {string} tableID Table ID to clear
 */
function clearTableProblemsTable(tableID) {
  clearTableProblems(tableID);
}

/**
 * Makes REST POST call to clear the problem details table
 *
 * @param {string} table Table ID to clear
 * @param {string} resource Resource of problem
 * @param {string} type Type of problem
 */
function clearDetailsProblemsTable(table, resource, type) {
  clearDetailsProblems(table, resource, type);
}

/**
 * Generates the problem summary table
 */
function refreshProblemSummaryTable() {
  if (problemSummaryTable) problemSummaryTable.ajax.reload(null, false); // user paging is not reset on reload
}

/**
 * Generates the problem details table
 */
function refreshProblemDetailsTable() {
  if (problemDetailTable) problemDetailTable.ajax.reload(null, false); // user paging is not reset on reload
}
