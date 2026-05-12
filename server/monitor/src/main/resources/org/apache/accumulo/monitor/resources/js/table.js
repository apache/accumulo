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
var tabletServersTable;
var tabletsTable;
var tabletsUrl;

/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshTable() {
  if (tableServersTable) {
    ajaxReloadTable(tableServersTable);
  }
  if (tabletsUrl && tabletServersTable && tabletsTable) {
    refreshTabletTables();
  }
}

/**
 * @returns {String} the tablet server host and port parsed from the given location string, or "UNASSIGNED" if unavailable
 */
function tabletServerFromLocation(location) {
  if (!location) {
    return 'UNASSIGNED';
  }

  if (location.startsWith('CURRENT:')) {
    return location.substring('CURRENT:'.length);
  }

  if (location.startsWith('FUTURE:')) {
    return location.substring('FUTURE:'.length);
  }

  const separator = location.indexOf(':');
  if (separator < 0 || separator + 1 >= location.length) {
    return location;
  }

  return location.substring(separator + 1);
}

/**
 * Roll up per-tablet counts by tablet server.
 */
function summarizeTabletsByServer(tablets) {
  const summaries = new Map();

  tablets.forEach(function (tablet) {
    const server = tabletServerFromLocation(tablet.location);
    let summary = summaries.get(server);
    if (summary === undefined) {
      summary = {
        tabletServer: server,
        tabletCount: 0,
        estimatedEntries: 0,
        estimatedSize: 0,
        numFiles: 0,
        numWalLogs: 0
      };
      summaries.set(server, summary);
    }

    summary.tabletCount += 1;
    summary.estimatedEntries += tablet.estimatedEntries ?? 0;
    summary.estimatedSize += tablet.estimatedSize ?? 0;
    summary.numFiles += tablet.numFiles ?? 0;
    summary.numWalLogs += tablet.numWalLogs ?? 0;
  });

  return Array.from(summaries.values());
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
function refreshTabletTables() {
  console.debug('Fetching tablets info from: ' + tabletsUrl);

  $.getJSON(tabletsUrl, function (data) {
    var tablets = Array.isArray(data) ? data : [];

    tabletServersTable.clear();
    tabletServersTable.rows.add(summarizeTabletsByServer(tablets));
    tabletServersTable.draw(false);

    tabletsTable.clear();
    tabletsTable.rows.add(tablets);
    tabletsTable.draw(false);
  });
}

/**
 * Initializes the tablet details table.
 */
function initTabletsTable() {
  tabletsTable = $('#tabletsList').DataTable({
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
 * Initializes the tablet server rollup table.
 */
function initTabletServersTable() {
  tabletServersTable = $('#tabletServersList').DataTable({
    "stateSave": true,
    "searching": false,
    "paging": false,
    "info": false,
    "order": [
      [0, 'asc']
    ],
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
        "data": "tabletCount",
        "title": "Tablet Count"
      },
      {
        "data": "tabletServer",
        "title": "Tablet Server"
      },
      {
        "data": "estimatedEntries",
        "title": "Estimated Entries"
      },
      {
        "data": "estimatedSize",
        "title": "Estimated Size"
      },
      {
        "data": "numFiles",
        "title": "Files"
      },
      {
        "data": "numWalLogs",
        "title": "WALs"
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
  const url = contextPath + 'rest-v2/tables/' + tableId;
  tabletsUrl = contextPath + 'rest-v2/tables/' + tableId + '/tablets';
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

  initTabletServersTable();
  initTabletsTable();
  refreshTabletTables();
}
