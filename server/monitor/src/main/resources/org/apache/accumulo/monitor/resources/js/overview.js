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

var deploymentBreakdown = [];

/**
 * Creates overview initial table
 */
$(function () {
  // display datatables errors in the console instead of in alerts
  $.fn.dataTable.ext.errMode = 'throw';
  $('#deployment-rg-filter').on('keyup', function () {
    var input = this.value;
    if (isValidRegex(input) || input === '') {
      $('#deployment-rg-feedback').hide();
      $(this).removeClass('is-invalid');
      renderDeploymentMatrix(deploymentBreakdown, input);
    } else {
      $('#deployment-rg-feedback').show();
      $(this).addClass('is-invalid');
    }
  });

  refreshOverview();
});

/**
 * Makes the REST calls, generates the table with the new information
 */
function refreshOverview() {
  refreshDeploymentTables();
}

/**
 * Used to redraw the page
 */
function refresh() {
  refreshOverview();
}

/**
 * Refreshes the deployment overview tables
 */
function refreshDeploymentTables() {
  getDeployment().then(function () {
    var data = JSON.parse(sessionStorage.deployment);
    var breakdown = Array.isArray(data.breakdown) ? data.breakdown : [];
    var filterValue = $('#deployment-rg-filter').val() || '';
    deploymentBreakdown = breakdown;

    if (breakdown.length === 0) {
      $('#deploymentWarning').html('<div class="alert alert-warning" role="alert">' +
        'No deployment data is currently available.</div>');
    } else {
      $('#deploymentWarning').empty();
    }

    renderDeploymentMatrix(breakdown, filterValue);
  });
}

function renderDeploymentMatrix(breakdown, resourceGroupFilter) {
  var matrixData = buildDeploymentMatrix(breakdown, resourceGroupFilter);
  var $container = $('#deploymentBreakdownMatrix');

  if (breakdown.length === 0) {
    $container.empty();
    return;
  }

  if (matrixData.filteredResourceGroups.length === 0) {
    $container.html(buildDeploymentEmptyState(resourceGroupFilter));
    return;
  }

  $container.html(buildDeploymentMatrixTable(matrixData));
}

function buildDeploymentMatrix(breakdown, resourceGroupFilter) {
  var serverTypes = [];
  var serverTypeSet = new Set();
  var resourceGroups = new Map();
  var filterRegex = createRegex(resourceGroupFilter);

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
  var filteredResourceGroups = sortedResourceGroups.filter(function (resourceGroup) {
    if (filterRegex === null) {
      return true;
    }
    return filterRegex.test(resourceGroup);
  });

  return {
    serverTypes: serverTypes,
    resourceGroups: resourceGroups,
    filteredResourceGroups: filteredResourceGroups
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
      buildDeploymentCell(rowTotals) + '</td>');

    return '<tr>' + cells.join('') + '</tr>';
  }).join('');

  var footerCells = ['<th scope="row" class="deployment-matrix-group deployment-matrix-total">Total</th>'];

  matrixData.serverTypes.forEach(function (serverType) {
    footerCells.push('<td class="deployment-matrix-cell deployment-matrix-total">' +
      buildDeploymentCell(totalByServerType.get(serverType)) + '</td>');
  });
  footerCells.push('<td class="deployment-matrix-cell deployment-matrix-total">' +
    buildDeploymentCell(grandTotals) + '</td>');

  return '<div class="table-responsive">' +
    '<table class="table table-bordered table-sm align-middle deployment-matrix-table mb-0">' +
    '<thead><tr><th colspan="' + sanitize(String(matrixData.serverTypes.length + 2)) +
    '" class="center">Deployment Breakdown</th></tr><tr>' + headerCells.join('') +
    '</tr></thead><tbody>' + rowsHtml + '</tbody><tfoot><tr>' + footerCells.join('') +
    '</tr></tfoot></table></div>';
}

function buildDeploymentCell(counts) {
  var badgeClass = getDeploymentBadgeClass(counts.responding, counts.total);
  var label = counts.responding + '/' + counts.total;

  return '<span class="badge rounded-pill deployment-count-badge ' + badgeClass + '">' +
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

function buildDeploymentEmptyState(resourceGroupFilter) {
  if (resourceGroupFilter && resourceGroupFilter !== '') {
    return '<div class="alert alert-secondary mb-0" role="alert">' +
      'No resource groups match the current filter.</div>';
  }

  return '<div class="alert alert-secondary mb-0" role="alert">' +
    'No deployment breakdown data is currently available.</div>';
}

function createRegex(pattern) {
  if (!pattern) {
    return null;
  }

  try {
    return new RegExp(pattern, 'i');
  } catch (error) {
    return null;
  }
}
