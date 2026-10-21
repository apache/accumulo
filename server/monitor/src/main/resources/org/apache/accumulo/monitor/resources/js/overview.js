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

const deploymentTable = '#deployment-table';

var deploymentBreakdown = [];

/**
 * Creates overview initial table
 */
$(function () {
  // display datatables errors in the console instead of in alerts
  $.fn.dataTable.ext.errMode = 'throw';
  refreshOverview();
});

/**
 * Makes the REST calls, generates the table with the new information
 */
function refreshOverview() {
  $.when(getDeployment(), getInstanceOverview()).then(function () {
    refreshInstanceOverviewTables();
    refreshDeploymentTables();
  });
}

/**
 * Used to redraw the page
 */
function refresh() {
  refreshOverview();
}

function refreshInstanceOverviewTables() {
  var data = getStoredJson(INSTANCE_OVERVIEW, {});

  renderOverviewList('#instance-overview-list', [
    ["Namespaces", bigNumberForQuantity(data.numNamespaces || 0)],
    ["Tables", bigNumberForQuantity(data.numTables || 0)],
    ["Tablets", bigNumberForQuantity(data.numTablets || 0)],
    ["Entries", bigNumberForQuantity(data.numKVs || 0)],
    ["Files", bigNumberForQuantity(data.numFiles || 0)],
    ["File Size", bigNumberForSize(data.totalFileSize || 0)],
    ["Dead TServer Tablets", bigNumberForQuantity(data.tabletsAssignedToDeadTServers || 0)],
    ["Suspended Tablets", bigNumberForQuantity(data.totalSuspendedTablets || 0)],
    ["Recovery Tablets", bigNumberForQuantity(data.tabletsNeedingRecovery || 0)],
    ["Fate Tx Queued", bigNumberForQuantity(data.totalFateSubmitted || 0)],
    ["Fate Tx Running", bigNumberForQuantity(data.totalFateRunning || 0)],
    ["Servers Low On Memory", bigNumberForQuantity(data.totalServersLowMem || 0)]
  ]);

  renderOverviewList('#ingest-overview-list', [
    ["Entries", bigNumberForQuantity(data.ingestTotalEntries || 0)],
    ["Bytes", bigNumberForSize(data.ingestTotalEntriesBytes || 0)],
    ["Entries In Memory", bigNumberForQuantity(data.ingestTotalEntriesInMem || 0)],
    ["TServers Holding For MinC", bigNumberForQuantity(data.ingestNumTServersHolding || 0)],
    ["MinC Queued", bigNumberForQuantity(data.totalMinCQueued || 0)],
    ["MinC Running", bigNumberForQuantity(data.totalMinCRunning || 0)],
    ["MinC Completed", bigNumberForQuantity(data.totalMinCCompleted || 0)],
    ["Bulk Imports Queued", bigNumberForQuantity(data.ingestBulkImportQueued || 0)],
    ["Bulk Imports Running", bigNumberForQuantity(data.ingestBulkImportRunning || 0)]
  ]);

  renderOverviewList('#scan-overview-list', [
    ["Scans In Progress", bigNumberForQuantity(data.scansTotalInProgress || 0)],
    ["Entries Scanned", bigNumberForQuantity(data.scansTotalKvScanned || 0)],
    ["Entries Returned", bigNumberForQuantity(data.scansTotalKvReturned || 0)],
    ["Bytes Returned", bigNumberForSize(data.scansTotalKvReturnedBytes || 0)],
    ["Open Files", bigNumberForQuantity(data.scanTotalOpenFiles || 0)]
  ]);

  renderOverviewList('#compaction-overview-list', [
    ["MajC Queued", bigNumberForQuantity(data.compactionsQueued || 0)],
    ["MajC Dequeued", bigNumberForQuantity(data.compactionsDequeued || 0)],
    ["MajC Running", bigNumberForQuantity(data.compactionsRunning || 0)],
    ["MajC Failed", bigNumberForQuantity(data.compactionsFailed || 0)]
  ]);
}

function renderOverviewList(selector, rows) {
  var list = $(selector);
  list.empty();

  list.append(rows.map(function (row) {
    const name = row[0];
    const value = row[1];
    return createOverviewListItem(name, value);
  }));
}

function createOverviewListItem(name, rowValue) {
  var item = $(document.createElement("li"));
  item.addClass("list-group-item d-flex justify-content-between align-items-center");

  var label = $(document.createElement("span"));
  label.text(name);
  item.append(label);

  var value = $(document.createElement("strong"));
  value.addClass("text-end text-nowrap");
  value.text(rowValue);
  item.append(value);

  return item;
}

/**
 * Refreshes the deployment overview tables
 */
function refreshDeploymentTables() {
  var data = JSON.parse(sessionStorage.deployment);
  var breakdown = Array.isArray(data.breakdown) ? data.breakdown : [];
  deploymentBreakdown = breakdown;

  if (breakdown.length === 0) {
    $('#deploymentWarning').html('<div class="alert alert-warning" role="alert">' +
      'No deployment data is currently available.</div>');
  } else {
    $('#deploymentWarning').empty();
  }

  renderDeploymentMatrix(breakdown);
}

function renderDeploymentMatrix(breakdown) {
  var matrixData = buildDeploymentMatrix(breakdown);
  var $container = $(deploymentTable);

  if (breakdown.length === 0) {
    $container.empty();
    return;
  }

  if (matrixData.filteredResourceGroups.length === 0) {
    $container.html(buildDeploymentEmptyState(resourceGroupFilter));
    return;
  }

  $container.find('thead').remove();
  $container.find('tbody').remove();
  $container.html(buildDeploymentMatrixTable(matrixData));
}

function buildDeploymentMatrix(breakdown) {
  var serverTypes = [];
  var serverTypeSet = new Set();
  var resourceGroups = new Map();

  breakdown.forEach(function (row) {
    if (serverTypeSet.has(row.serverType) === false) {
      serverTypeSet.add(row.serverType);
      serverTypes.push(row.serverType);
    }

    if (resourceGroups.has(row.resourceGroup) === false) {
      resourceGroups.set(row.resourceGroup, new Map());
    }

    resourceGroups.get(row.resourceGroup).set(row.serverType, {
      responding: Number(row.responding),
      total: Number(row.total)
    });
  });

  var sortedResourceGroups = Array.from(resourceGroups.keys()).sort();

  return {
    serverTypes: serverTypes,
    resourceGroups: resourceGroups,
    filteredResourceGroups: sortedResourceGroups
  };
}

function buildDeploymentMatrixTable(matrixData) {
  var headerCells = ['<th class="deployment-matrix-group deployment-matrix-header">Resource Group</th>'];
  var totalByServerType = new Map();
  var grandTotals = {
    responding: 0,
    total: 0
  };

  matrixData.serverTypes.forEach(function (serverType) {
    headerCells.push('<th class="deployment-matrix-cell deployment-matrix-header">' +
      sanitize(serverType) + '</th>');
    totalByServerType.set(serverType, {
      responding: 0,
      total: 0
    });
  });
  headerCells.push('<th class="deployment-matrix-cell deployment-matrix-header">Total</th>');

  var rowsHtml = matrixData.filteredResourceGroups.map(function (resourceGroup) {
    var cells = ['<th scope="row" class="deployment-matrix-group">' + sanitize(resourceGroup) + '</th>'];
    var rowTotals = {
      responding: 0,
      total: 0
    };
    var rowData = matrixData.resourceGroups.get(resourceGroup);

    matrixData.serverTypes.forEach(function (serverType) {
      var counts = rowData.get(serverType) || {
        responding: 0,
        total: 0
      };
      var totals = totalByServerType.get(serverType);

      totals.responding += counts.responding;
      totals.total += counts.total;
      rowTotals.responding += counts.responding;
      rowTotals.total += counts.total;

      cells.push('<td class="deployment-matrix-cell">' + buildDeploymentCell(counts) + '</td>');
    });

    grandTotals.responding += rowTotals.responding;
    grandTotals.total += rowTotals.total;
    cells.push('<td class="deployment-matrix-cell deployment-matrix-total">' +
      buildDeploymentTotalCell(rowTotals) + '</td>');

    return '<tr>' + cells.join('') + '</tr>';
  }).join('');

  var footerCells = ['<th scope="row" class="deployment-matrix-group deployment-matrix-total">Total</th>'];

  matrixData.serverTypes.forEach(function (serverType) {
    footerCells.push('<td class="deployment-matrix-cell deployment-matrix-total">' +
      buildDeploymentTotalCell(totalByServerType.get(serverType)) + '</td>');
  });
  footerCells.push('<td class="deployment-matrix-cell deployment-matrix-total">' +
    buildDeploymentTotalCell(grandTotals) + '</td>');

  return '<thead><tr>' + headerCells.join('') +
    '</tr></thead><tbody>' + rowsHtml + '</tbody><tfoot><tr>' + footerCells.join('') +
    '</tr></tfoot>';
}

/**
 * Builds the HTML for a badge containing the counts of responding vs total servers
 */
function buildDeploymentCell(counts, neutral) {
  var badgeClass = getDeploymentBadgeClass(counts.responding, counts.total);
  var label = counts.responding + '/' + counts.total;

  return '<span class="badge rounded-pill deployment-count-badge ' + badgeClass + '">' +
    sanitize(label) + '</span>';
}
/**
 * Builds the HTML for a badge containing the total counts of responding vs total servers
 */
function buildDeploymentTotalCell(counts) {
  var label = counts.responding + '/' + counts.total;

  return '<span class="badge rounded-pill deployment-count-badge deployment-count-total">' +
    sanitize(label) + '</span>';
}

function getDeploymentBadgeClass(responding, total) {
  if (total === 0) {
    return 'deployment-count-empty';
  }

  if (responding === 0) {
    return 'deployment-count-error';
  }

  if (responding === total) {
    return 'deployment-count-ok';
  }

  return 'deployment-count-warn';
}

function buildDeploymentEmptyState() {
  return '<div class="alert alert-secondary mb-0" role="alert">' +
    'No deployment breakdown data is currently available.</div>';
}
