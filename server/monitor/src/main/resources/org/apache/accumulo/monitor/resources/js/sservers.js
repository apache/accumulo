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
/* JSLint global definitions */
/*global
    $, sessionStorage, timeDuration, bigNumberForQuantity, bigNumberForSize, ajaxReloadTable,
    getGroups, getSserversDetail
*/
"use strict";

var sserversTable;
var SSERVERS_SESSION_KEY = 'sservers';

var METRICS = {
  OPEN_FILES: 'accumulo.scan.files.open',
  QUERIES: 'accumulo.scan.queries',
  SCANNED_ENTRIES: 'accumulo.scan.query.scanned.entries',
  QUERY_RESULTS: 'accumulo.scan.query.results',
  QUERY_RESULTS_BYTES: 'accumulo.scan.query.results.bytes',
  BUSY_TIMEOUTS: 'accumulo.scan.busy.timeout.count',
  RESERVATION_CONFLICTS: 'accumulo.scan.reservation.conflict.count',
  ZOMBIE_THREADS: 'accumulo.scan.zombie.threads'
};

function normalizeStatistic(value) {
  if (value === null || value === undefined) {
    return null;
  }
  return String(value).toLowerCase();
}

function extractStatistic(tags) {
  if (!tags) {
    return null;
  }
  for (var i = 0; i < tags.length; i++) {
    var tag = tags[i];
    for (var key in tag) {
      if (Object.prototype.hasOwnProperty.call(tag, key) && key === 'statistic') {
        return normalizeStatistic(tag[key]);
      }
    }
  }
  return null;
}

function getMetricValue(metrics, name, statistic) {
  if (!metrics) {
    return 0;
  }
  var desiredStatistic = normalizeStatistic(statistic);
  for (var i = 0; i < metrics.length; i++) {
    var metric = metrics[i];
    if (metric.name !== name) {
      continue;
    }
    var metricStatistic = extractStatistic(metric.tags);
    if (!desiredStatistic) {
      if (metricStatistic === null || metricStatistic === 'value' || metricStatistic === 'count') {
        return metric.value;
      }
    } else if (metricStatistic === desiredStatistic) {
      return metric.value;
    }
  }
  return 0;
}

function buildScanServerRow(response) {
  var metrics = response.metrics || [];
  return {
    host: response.host,
    resourceGroup: response.resourceGroup,
    lastContact: Date.now() - response.timestamp,
    openFiles: getMetricValue(metrics, METRICS.OPEN_FILES),
    queries: getMetricValue(metrics, METRICS.QUERIES),
    scannedEntries: getMetricValue(metrics, METRICS.SCANNED_ENTRIES),
    queryResults: getMetricValue(metrics, METRICS.QUERY_RESULTS),
    queryResultBytes: getMetricValue(metrics, METRICS.QUERY_RESULTS_BYTES),
    busyTimeouts: getMetricValue(metrics, METRICS.BUSY_TIMEOUTS),
    reservationConflicts: getMetricValue(metrics, METRICS.RESERVATION_CONFLICTS),
    zombieThreads: getMetricValue(metrics, METRICS.ZOMBIE_THREADS)
  };
}

function getStoredRows() {
  if (!sessionStorage[SSERVERS_SESSION_KEY]) {
    return [];
  }
  return JSON.parse(sessionStorage[SSERVERS_SESSION_KEY]).servers;
}

function setStoredRows(rows) {
  sessionStorage[SSERVERS_SESSION_KEY] = JSON.stringify({
    servers: rows
  });
}

function buildRowsFromSessionStorage(groups) {
  var rows = [];

  groups.forEach(function (group) {
    var sessionKey = 'sserversDetail_' + group;
    if (!sessionStorage[sessionKey]) {
      return;
    }
    var responses = JSON.parse(sessionStorage[sessionKey]);
    if (!Array.isArray(responses)) {
      return;
    }
    rows = rows.concat(responses.map(buildScanServerRow));
  });

  return rows;
}

function refreshScanServersTable() {
  ajaxReloadTable(sserversTable);
}

function refreshScanServers() {
  getGroups().then(function () {
    var groupList = JSON.parse(sessionStorage.groups || '[]');

    if (!Array.isArray(groupList) || groupList.length === 0) {
      setStoredRows([]);
      refreshScanServersTable();
      return;
    }

    var requests = $.map(groupList, function (group) {
      return getSserversDetail(group);
    });

    $.when.apply($, requests).done(function () {
      setStoredRows(buildRowsFromSessionStorage(groupList));
      refreshScanServersTable();
    }).fail(function () {
      setStoredRows([]);
      refreshScanServersTable();
    });
  }).fail(function () {
    setStoredRows([]);
    refreshScanServersTable();
  });
}

function refresh() {
  refreshScanServers();
}

$(function () {
  setStoredRows([]);

  sserversTable = $('#sservers').DataTable({
    "autoWidth": false,
    "ajax": function (data, callback) {
      callback({
        data: getStoredRows()
      });
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
      },
      {
        "targets": "duration",
        "render": function (data, type) {
          if (type === 'display') {
            data = timeDuration(data);
          }
          return data;
        }
      }
    ],
    "columns": [{
        "data": "host"
      },
      {
        "data": "resourceGroup"
      },
      {
        "data": "lastContact"
      },
      {
        "data": "openFiles"
      },
      {
        "data": "queries"
      },
      {
        "data": "scannedEntries"
      },
      {
        "data": "queryResults"
      },
      {
        "data": "queryResultBytes"
      },
      {
        "data": "busyTimeouts"
      },
      {
        "data": "reservationConflicts"
      },
      {
        "data": "zombieThreads"
      }
    ]
  });

  refreshScanServers();
});
