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
 * Used to set the refresh interval to 5 seconds
 */
function refresh() {
  clearInterval(TIMER);
  if (sessionStorage.autoRefresh == 'true') {
    TIMER = setInterval('refreshProblems()', 5000);
  }
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
    items.push('<td class="center" colspan="5"><i>Empty</i></td>');
    $('<tr/>', {
      html: items.join('')
    }).appendTo('#problemSummary');
  } else {
    $.each(data.problemSummary, function(key, val) {
      var items = [];
      items.push('<td class="firstcell left"><a href="/problems?table=' +
          val.tableID.split('+').join('%2B') + '">' + val.tableName +
          '</a></td>');

      items.push('<td class="right">' + bigNumberForQuantity(val.fileRead) +
          '</td>');

      items.push('<td class="right">' + bigNumberForQuantity(val.fileWrite) +
          '</td>');

      items.push('<td class="right">' + bigNumberForQuantity(val.tableLoad) +
          '</td>');
      items.push('<td><a href="javascript:clearTableProblemsTable(\'' +
          val.tableID + '\');">clear ALL ' + val.tableName +
          ' problems</a></td>');

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
    items.push('<td class="center" colspan="7"><i>Empty</i></td>');
    $('<tr/>', {
      html: items.join('')
    }).appendTo('#problemDetails');
  } else {
    $.each(data.problemDetails, function(key, val) {
      var items = [];
      // Filters the details problems for the selected tableID
      if (tableID === val.tableID || tableID === '') {
        items.push('<td class="firstcell left" data-value="' + val.tableName +
            '"><a href="/tables/' + val.tableID + '">' + val.tableName +
            '</a></td>');

        items.push('<td class="right" data-value="' + val.type + '">' +
            val.type + '</td>');

        items.push('<td class="right" data-value="' + val.server + '">' +
            val.server + '</td>');

        var date = new Date(val.time);
        items.push('<td class="right" data-value="' + val.time + '">' +
            date.toLocaleString() + '</td>');

        items.push('<td class="right" data-value="' + val.resource + '">' +
            val.resource + '</td>');

        items.push('<td class="right" data-value="' + val.exception + '">' +
            val.exception + '</td>');

        items.push('<td><a href="javascript:clearDetailsProblemsTable(\'' +
            val.tableID + '\', \'' + val.resource + '\', \'' + val.type +
            '\')">clear this problem</a></td>');
      }

      $('<tr/>', {
        html: items.join('')
      }).appendTo('#problemDetails');

    });
  }
}

/**
 * Sorts the problemDetails table on the selected column
 *
 * @param {number} n Column number to sort by
 */
function sortTable(n) {
  if (sessionStorage.tableColumnSort !== undefined &&
      sessionStorage.tableColumnSort == n &&
      sessionStorage.direction !== undefined) {
    direction = sessionStorage.direction === 'asc' ? 'desc' : 'asc';
  } else {
    direction = sessionStorage.direction === undefined ?
        'asc' : sessionStorage.direction;
  }

  sessionStorage.tableColumnSort = n;

  sortTables('problemDetails', direction, n);
}

/**
 * Creates the problem summary header
 */
function createSummaryHeader() {
  var caption = [];

  caption.push('<span class="table-caption">Problem&nbsp;Summary</span><br />');

  $('<caption/>', {
    html: caption.join('')
  }).appendTo('#problemSummary');

  var items = [];

  items.push('<th class="firstcell">Table&nbsp;</th>');
  items.push('<th>FILE_READ&nbsp;</th>');
  items.push('<th>FILE_WRITE&nbsp;</th>');
  items.push('<th>TABLET_LOAD&nbsp;</th>');
  items.push('<th>Operations&nbsp;</th>');

  $('<tr/>', {
      html: items.join('')
  }).appendTo('#problemSummary');
}

var tableID;
/**
 * Creates the problem detail header
 *
 * @param {string} table Table ID of problem
 */
function createDetailsHeader(table) {
  tableID = table;
  var caption = [];

  caption.push('<span class="table-caption">Problem&nbsp;Details</span><br />');
  caption.push('<span class="table-subcaption">Problems' +
      '&nbsp;identified&nbsp;with&nbsp;tables.</span><br />');

  $('<caption/>', {
    html: caption.join('')
  }).appendTo('#problemDetails');

  var items = [];

  items.push('<th class="firstcell" onclick="sortTable(0)">Table&nbsp;</th>');
  items.push('<th onclick="sortTable(1)">Problem&nbsp;Type&nbsp;</th>');
  items.push('<th onclick="sortTable(2)">Server&nbsp;</th>');
  items.push('<th onclick="sortTable(3)">Time&nbsp;</th>');
  items.push('<th onclick="sortTable(4)">Resource&nbsp;</th>');
  items.push('<th onclick="sortTable(5)">Exception&nbsp;</th>');
  items.push('<th>Operations&nbsp;</th>');

  $('<tr/>', {
    html: items.join('')
  }).appendTo('#problemDetails');
}
