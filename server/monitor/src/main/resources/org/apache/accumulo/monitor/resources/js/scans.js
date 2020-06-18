/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Creates scans initial table
 */
$(document).ready(function() {
  refreshScans();
});

/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshScans() {
  getScans().then(function() {
    refreshScansTable();
  });
}

/**
 * Used to redraw the page
 */
function refresh() {
  refreshScans();
}

/**
 * Generates the scans table
 */
function refreshScansTable() {
  clearTableBody('scanStatus');

  var data = sessionStorage.scans === undefined ?
      [] : JSON.parse(sessionStorage.scans);

  if (data.length === 0 || data.scans.length === 0) {
    var items = createEmptyRow(3, 'Empty');

    $('<tr/>', {
      html: items
    }).appendTo('#scanStatus tbody');
  } else {
    $.each(data.scans, function(key, val) {
      var items = [];

      items.push(createFirstCell(val.server,
          '<a href="/tservers?s=' + val.server + '">' + val.server +
          '</a>'));

      items.push(createRightCell(val.scanCount, val.scanCount));

      items.push(createRightCell(val.oldestScan, timeDuration(val.oldestScan)));

      $('<tr/>', {
        html: items.join('')
      }).appendTo('#scanStatus tbody');
    });
  }
}
