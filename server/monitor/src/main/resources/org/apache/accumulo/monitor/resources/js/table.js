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

var tableServersTable;
var tabletsTable;

/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshTable() {
  ajaxReloadTable(tableServersTable);
}

/**
 * Used to redraw the page
 */
function refresh() {
  refreshTable();
}

/**
 * Makes the REST call to fetch tablet details and render them.
 */
function initTabletsTable(tableId) {
  var tabletsUrl = '/rest-v2/tables/' + tableId + '/tablets';
  console.debug('Fetching tablets info from: ' + tabletsUrl);

  tabletsTable = $('#tabletsList').DataTable({
    "ajax": {
      "url": tabletsUrl,
      "dataSrc": ""
    },
    "stateSave": true,
    "columnDefs": [{
        "targets": "big-num",
        "render": function (data, type) {
          if (type === 'display') {
            data = bigNumberForQuantity(data);
          }
          return data;
        }
      },
      {
        "targets": "big-size",
        "render": function (data, type) {
          if (type === 'display') {
            data = bigNumberForSize(data);
          }
          return data;
        }
      }
    ],
    "columns": [{
        "data": "tabletId",
        "title": "Tablet ID"
      },
      {
        "data": "estimatedSize",
        "title": "Estimated Size"
      },
      {
        "data": "estimatedEntries",
        "title": "Estimated Entries"
      },
      {
        "data": "tabletAvailability",
        "title": "Availability"
      },
      {
        "data": "numFiles",
        "title": "Files"
      },
      {
        "data": "numWalLogs",
        "title": "WALs"
      },
      {
        "data": "location",
        "title": "Location"
      }
    ]
  });
}

/**
 * Initialize the table
 * 
 * @param {String} tableId the accumulo table ID
 */
function initTableServerTable(tableId) {
  const url = '/rest-v2/tables/' + tableId;
  console.debug('REST url used to fetch summary data: ' + url);

  tableServersTable = $('#participatingTServers').DataTable({
    "ajax": {
      "url": url,
      "dataSrc": function (json) {
        // Convert the JSON object into an array for DataTables consumption.
        return [json];
      }
    },
    "stateSave": true,
    "searching": false,
    "paging": false,
    "info": false,
    "columnDefs": [{
        "targets": "big-num",
        "render": function (data, type) {
          if (type === 'display') {
            data = bigNumberForQuantity(data);
          }
          return data;
        }
      },
      {
        "targets": "big-size",
        "render": function (data, type) {
          if (type === 'display') {
            data = bigNumberForSize(data);
          }
          return data;
        }
      }
    ],
    "columns": [{
        "data": "totalEntries",
        "title": "Entry Count"
      },
      {
        "data": "totalSizeOnDisk",
        "title": "Size on disk"
      },
      {
        "data": "totalFiles",
        "title": "File Count"
      },
      {
        "data": "totalWals",
        "title": "WAL Count"
      },
      {
        "data": "totalTablets",
        "title": "Total Tablet Count"
      },
      {
        "data": "availableAlways",
        "title": "Always Hosted Count"
      },
      {
        "data": "availableOnDemand",
        "title": "On Demand Count"
      },
      {
        "data": "availableNever",
        "title": "Never Hosted Count"
      },
      {
        "data": "totalAssignedTablets",
        "title": "Assigned Tablet Count"
      },
      {
        "data": "totalAssignedToDeadServerTablets",
        "title": "Tablets Assigned to Dead Servers Count"
      },
      {
        "data": "totalHostedTablets",
        "title": "Hosted Tablet Count"
      },
      {
        "data": "totalSuspendedTablets",
        "title": "Suspended Tablet Count"
      },
      {
        "data": "totalUnassignedTablets",
        "title": "Unassigned Tablet Count"
      }
    ]
  });

  refreshTable();
  initTabletsTable(tableId);
}
