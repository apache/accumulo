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
  getStatus().then(function () {
    var managerStatus = JSON.parse(sessionStorage.status).managerStatus;
    // If the manager is down, show only the first row, otherwise refresh old values
    $('#manager tr td').hide();
    if (managerStatus === 'ERROR') {
      $('#manager tr td:first').show();
    } else {
      $('#manager tr td:not(:first)').show();
      refreshManagerTable();
    }
  });

  refreshDeploymentTables();
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
  getManager().then(function () {
    var data = JSON.parse(sessionStorage.manager);
    var table = $('#manager td.right');

    table.eq(0).html(data.host);
    table.eq(1).html(data.resourceGroup);
    table.eq(2).html(dateFormat(data.timestamp));
    table.eq(3).html('<a href="' + contextPath + 'rest-v2/manager/metrics">Metrics</a>');
  });
}

/**
 * Refreshes the deployment overview tables
 */
function refreshDeploymentTables() {
  getDeployment().then(function () {
    var data = JSON.parse(sessionStorage.deployment);
    var groups = Array.isArray(data.groups) ? data.groups : [];
    if (groups.length === 0) {
      $('#deploymentTables').html('<div class="alert alert-warning" role="alert">' +
        'No deployment data is currently available.</div>');
      return;
    }

    var tables = groups.map(renderDeploymentTable).join('');

    $('#deploymentTables').html(tables);
  });
}

/**
 * Renders the deployment table for one resource group
 *
 * @param {object} group deployment data for one resource group
 * @returns {string} html for the resource group deployment table
 */
function renderDeploymentTable(group) {
  var processes = Array.isArray(group.processes) ? group.processes : [];
  var rows = processes.map(function (process) {
    return '<tr>' +
      '<td>' + process.serverType + '</td>' +
      '<td>' + process.total + '</td>' +
      '<td>' + process.responding + '</td>' +
      '<td>' + process.notResponding + '</td>' +
      '</tr>';
  }).join('');

  return '<div class="mb-4">' +
    '<table class="table table-bordered table-striped table-condensed" style="table-layout: fixed; width: 100%;">' +
    '<colgroup>' +
    '<col style="width: 25%;">' +
    '<col style="width: 25%;">' +
    '<col style="width: 25%;">' +
    '<col style="width: 25%;">' +
    '</colgroup>' +
    '<thead>' +
    '<tr><th colspan="4" class="center">' + group.resourceGroup + '</th></tr>' +
    '<tr>' +
    '<th>Process</th>' +
    '<th>Total</th>' +
    '<th>Responding</th>' +
    '<th>Not Responding</th>' +
    '</tr>' +
    '</thead>' +
    '<tbody>' + rows + '</tbody>' +
    '</table>' +
    '</div>';
}
