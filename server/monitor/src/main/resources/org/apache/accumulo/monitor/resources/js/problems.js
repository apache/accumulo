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
var tableID;

/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshProblems() {
  $.ajaxSetup({
    async: false
  });
  getProblems();
  $.ajaxSetup({
    async: true
  });
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
  refreshProblems();
  refreshNavBar();
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
  refreshProblems();
  refreshNavBar();
}

/**
 * Generates the problem summary table
 */
function refreshProblemSummaryTable() {
  clearTable('problemSummary');
  var data = sessionStorage.problemSummary === undefined ?
      [] : JSON.parse(sessionStorage.problemSummary);

  if (data.length === 0 || Object.keys(data.problemSummary).length === 0) {
    var items = [];
    items.push(createEmptyRow(5, 'Empty'));
    $('<tr/>', {
      html: items.join('')
    }).appendTo('#problemSummary');
  } else {
    $.each(data.problemSummary, function(key, val) {
      var items = [];
      items.push(createFirstCell('', '<a href="/problems?table=' +
          val.tableID.split('+').join('%2B') + '">' + val.tableName +
          '</a>'));

      items.push(createRightCell('', bigNumberForQuantity(val.fileRead)));

      items.push(createRightCell('', bigNumberForQuantity(val.fileWrite)));

      items.push(createRightCell('', bigNumberForQuantity(val.tableLoad)));
      items.push(createLeftCell('', '<a href="javascript:clearTableProblemsTable(\'' +
          val.tableID + '\');">clear ALL ' + val.tableName + ' problems</a>'));

      $('<tr/>', {
        html: items.join('')
      }).appendTo('#problemSummary');
    });
  }
}

/**
 * Generates the problem details table
 */
function refreshProblemDetailsTable() {
  clearTable('problemDetails');
  var data = sessionStorage.problemDetails === undefined ?
      [] : JSON.parse(sessionStorage.problemDetails);

  if (data.length === 0 || Object.keys(data.problemDetails).length === 0) {
    var items = [];
    items.push(createEmptyRow(7, 'Empty'));
    $('<tr/>', {
      html: items.join('')
    }).appendTo('#problemDetails');
  } else {
    $.each(data.problemDetails, function(key, val) {
      var items = [];
      // Filters the details problems for the selected tableID
      if (tableID === val.tableID || tableID === '') {
        items.push(createFirstCell(val.tableName,
            '<a href="/tables/' + val.tableID + '">' + val.tableName + '</a>'));

        items.push(createRightCell(val.type, val.type));

        items.push(createRightCell(val.server, val.server));

        var date = new Date(val.time);
        items.push(createRightCell(val.time, date.toLocaleString()));

        items.push(createRightCell(val.resource, val.resource));

        items.push(createRightCell(val.exception, val.exception));

        items.push(createLeftCell('', 
            '<a href="javascript:clearDetailsProblemsTable(\'' +
            val.tableID + '\', \'' + val.resource + '\', \'' + val.type +
            '\')">clear this problem</a>'));
      }

      $('<tr/>', {
        html: items.join('')
      }).appendTo('#problemDetails');

    });
  }
}
