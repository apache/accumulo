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
 * Creates overview initial table
 */
$(function () {
  refreshOverview();
});

/**
 * Makes the REST calls, generates the table with the new information
 */
function refreshOverview() {
  getManager().then(function () {
    refreshManagerTable();
  });
}

/**
 * Used to redraw the page
 */
function refresh() {
  refreshOverview();
}

/**
 * Refreshes the manager table
 */
function refreshManagerTable() {
  var data = sessionStorage.manager === undefined ? [] : JSON.parse(sessionStorage.manager);

  $('#manager tr td:first').hide();
  $('#manager tr td').hide();

  // If the manager is down, show the first row, otherwise refresh old values
  if (data.length === 0 || data.manager === 'No Managers running') {
    $('#manager tr td:first').show();
  } else {
    $('#manager tr td:not(:first)').show();
    var table = $('#manager td.right');

    table.eq(0).html(bigNumberForQuantity(data.tables));
    table.eq(1).html(bigNumberForQuantity(data.totalTabletServers));
    table.eq(2).html(bigNumberForQuantity(data.deadTabletServersCount));
    table.eq(3).html(bigNumberForQuantity(data.tablets));
    table.eq(4).html(bigNumberForQuantity(data.numentries));
    table.eq(5).html(bigNumberForQuantity(data.lookups));
    table.eq(6).html(timeDuration(data.uptime));
  }
}
