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

var deploymentSummaryTable;
var deploymentBreakdownTable;
const SERVER_TYPE_LINKS = new Map([
  ['Manager', 'manager'],
  ['Garbage Collector', 'gc'],
  ['Compactor', 'compactors'],
  ['Scan Server', 'sservers'],
  ['Tablet Server', 'tservers']
]);
const SUMMARY_SERVER_TYPES = ['Manager', 'Garbage Collector', 'Compactor', 'Scan Server',
  'Tablet Server'
];

/**
 * Creates overview initial table
 */
$(function () {
  // display datatables errors in the console instead of in alerts
  $.fn.dataTable.ext.errMode = 'throw';

  deploymentSummaryTable = $('#deploymentSummaryTable').DataTable({
    "autoWidth": false,
    "paging": false,
    "searching": false,
    "info": false,
    "ordering": false,
    "dom": 't',
    "data": [],
    "columnDefs": [{
        "targets": 0,
        "width": "60%"
      },
      {
        "targets": 1,
        "width": "40%"
      }
    ],
    "columns": [{
        "data": "serverType",
        "render": function (data, type) {
          if (type !== 'display') {
            return data;
          }

          var link = SERVER_TYPE_LINKS.get(data);
          if (link === undefined) {
            return data;
          }

          return '<a href="' + sanitize(link) + '">' + sanitize(data) + '</a>';
        }
      },
      {
        "data": null,
        "render": function (data, type, row) {
          return row.responding + ' / ' + row.total;
        }
      }
    ]
  });

  deploymentBreakdownTable = $('#deploymentBreakdownTable').DataTable({
    "autoWidth": false,
    "paging": true,
    "pageLength": 10,
    "searching": true,
    "info": true,
    "order": [
      [0, 'asc'],
      [1, 'asc']
    ],
    "dom": 't<"row"<"col-sm-3"l><"col-sm-5 text-center"i><"col-sm-4 text-right"p>>',
    "data": [],
    "columnDefs": [{
        "targets": 0,
        "width": "30%"
      },
      {
        "targets": 1,
        "width": "40%"
      },
      {
        "targets": 2,
        "width": "30%"
      }
    ],
    "columns": [{
        "data": "resourceGroup",
        "name": "resourceGroup"
      },
      {
        "data": "serverType"
      },
      {
        "data": null,
        "render": function (data, type, row) {
          return row.responding + ' / ' + row.total;
        }
      }
    ]
  });

  $('#deployment-rg-filter').on('keyup', function () {
    var input = this.value;
    if (isValidRegex(input) || input === '') {
      $('#deployment-rg-feedback').hide();
      $(this).removeClass('is-invalid');
      deploymentBreakdownTable
        .column('resourceGroup:name')
        .search(input, true, false)
        .draw();
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
    var summary = buildDeploymentSummary(breakdown);

    if (breakdown.length === 0) {
      $('#deploymentWarning').html('<div class="alert alert-warning" role="alert">' +
        'No deployment data is currently available.</div>');
    } else {
      $('#deploymentWarning').empty();
    }

    deploymentSummaryTable.clear().rows.add(summary).draw();
    deploymentBreakdownTable.clear().rows.add(breakdown).draw();
  });
}

function buildDeploymentSummary(breakdown) {
  if (breakdown.length === 0) {
    return [];
  }

  var totalsByType = new Map();

  SUMMARY_SERVER_TYPES.forEach(function (serverType) {
    totalsByType.set(serverType, {
      serverType: serverType,
      total: 0,
      responding: 0
    });
  });

  breakdown.forEach(function (row) {
    var existing = totalsByType.get(row.serverType);
    if (existing === undefined) {
      totalsByType.set(row.serverType, {
        serverType: row.serverType,
        total: row.total,
        responding: row.responding
      });
    } else {
      existing.total += row.total;
      existing.responding += row.responding;
    }
  });

  return Array.from(totalsByType.values());
}
