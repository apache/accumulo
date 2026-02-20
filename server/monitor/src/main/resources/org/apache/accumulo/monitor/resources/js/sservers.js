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
    getGroups, getSserversDetail, REST_V2_PREFIX
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
  ZOMBIE_THREADS: 'accumulo.scan.zombie.threads',
  SERVER_IDLE: 'accumulo.server.idle',
  LOW_MEMORY_DETECTED: 'accumulo.detected.low.memory',
  SCANS_PAUSED_FOR_MEMORY: 'accumulo.scan.paused.for.memory',
  SCANS_RETURNED_EARLY_FOR_MEMORY: 'accumulo.scan.return.early.for.memory'
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
    console.warn('Missing scan-server metrics array when reading metric: ' + name);
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
  console.warn('Missing expected scan-server metric: ' + name);
  return 0;
}

function buildScanServerRow(response) {
  var metrics = response.metrics || [];
  var hasMetrics = Array.isArray(metrics) && metrics.length > 0;
  var row = {
    host: response.host,
    resourceGroup: response.resourceGroup,
    lastContact: Date.now() - response.timestamp
  };

  if (!hasMetrics) {
    row.openFiles = null;
    row.queries = null;
    row.scannedEntries = null;
    row.queryResults = null;
    row.queryResultBytes = null;
    row.busyTimeouts = null;
    row.reservationConflicts = null;
    row.zombieThreads = null;
    row.serverIdle = null;
    row.lowMemoryDetected = null;
    row.scansPausedForMemory = null;
    row.scansReturnedEarlyForMemory = null;
    return row;
  }

  row.openFiles = getMetricValue(metrics, METRICS.OPEN_FILES);
  row.queries = getMetricValue(metrics, METRICS.QUERIES);
  row.scannedEntries = getMetricValue(metrics, METRICS.SCANNED_ENTRIES);
  row.queryResults = getMetricValue(metrics, METRICS.QUERY_RESULTS);
  row.queryResultBytes = getMetricValue(metrics, METRICS.QUERY_RESULTS_BYTES);
  row.busyTimeouts = getMetricValue(metrics, METRICS.BUSY_TIMEOUTS);
  row.reservationConflicts = getMetricValue(metrics, METRICS.RESERVATION_CONFLICTS);
  row.zombieThreads = getMetricValue(metrics, METRICS.ZOMBIE_THREADS);
  row.serverIdle = getMetricValue(metrics, METRICS.SERVER_IDLE);
  row.lowMemoryDetected = getMetricValue(metrics, METRICS.LOW_MEMORY_DETECTED);
  row.scansPausedForMemory = getMetricValue(metrics, METRICS.SCANS_PAUSED_FOR_MEMORY);
  row.scansReturnedEarlyForMemory = getMetricValue(metrics, METRICS.SCANS_RETURNED_EARLY_FOR_MEMORY);
  return row;
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
  var metricsUnavailable = false;

  groups.forEach(function (group) {
    var sessionKey = 'sserversDetail_' + group;
    if (!sessionStorage[sessionKey]) {
      return;
    }
    var responses = JSON.parse(sessionStorage[sessionKey]);
    if (!Array.isArray(responses)) {
      return;
    }
    for (var i = 0; i < responses.length; i++) {
      if (!Array.isArray(responses[i].metrics) || responses[i].metrics.length === 0) {
        metricsUnavailable = true;
        break;
      }
    }
    rows = rows.concat(responses.map(buildScanServerRow));
  });

  return {
    rows: rows,
    metricsUnavailable: metricsUnavailable
  };
}

function refreshScanServersTable() {
  ajaxReloadTable(sserversTable);
}

function refreshSserversBanner(metricsUnavailable) {
  return $.when(
    $.getJSON(REST_V2_PREFIX + '/sservers/summary'),
    $.getJSON(REST_V2_PREFIX + '/problems')
  ).done(function (summaryResp, problemsResp) {
    var summary = summaryResp && summaryResp[0] ? summaryResp[0] : {};
    var problems = problemsResp && problemsResp[0] ? problemsResp[0] : [];
    var hasSservers = summary && Object.keys(summary).length > 0;
    var hasProblemSserver = problems.some(function (problem) {
      return problem.type === 'SCAN_SERVER' || problem.serverType === 'SCAN_SERVER';
    });

    if (metricsUnavailable || hasProblemSserver) {
      var warnings = [];
      if (hasProblemSserver) {
        warnings.push('one or more scan servers are unavailable');
      }
      if (metricsUnavailable) {
        warnings.push('scan-server metrics are not present (are metrics enabled?)');
      }
      $('#sservers-banner-message')
        .removeClass('alert-danger')
        .addClass('alert-warning')
        .text('WARN: ' + warnings.join('; ') + '.');
      $('#sserversStatusBanner').show();
    } else if (!hasSservers) {
      $('#sserversStatusBanner').hide();
    } else {
      $('#sserversStatusBanner').hide();
    }
  }).fail(function () {
    $('#sservers-banner-message')
      .removeClass('alert-warning')
      .addClass('alert-danger')
      .text('ERROR: unable to retrieve scan-server status.');
    $('#sserversStatusBanner').show();
  });
}

function refreshScanServers() {
  getGroups().then(function () {
    var groupList = JSON.parse(sessionStorage.groups || '[]');

    if (!Array.isArray(groupList) || groupList.length === 0) {
      setStoredRows([]);
      refreshScanServersTable();
      refreshSserversBanner(false);
      return;
    }

    var requests = $.map(groupList, function (group) {
      return getSserversDetail(group);
    });

    $.when.apply($, requests).done(function () {
      var detailData = buildRowsFromSessionStorage(groupList);
      setStoredRows(detailData.rows);
      refreshScanServersTable();
      refreshSserversBanner(detailData.metricsUnavailable);
    }).fail(function () {
      setStoredRows([]);
      refreshScanServersTable();
      refreshSserversBanner(false);
    });
  }).fail(function () {
    setStoredRows([]);
    refreshScanServersTable();
    refreshSserversBanner(false);
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
            if (data === null || data === undefined) {
              return '&mdash;';
            }
            data = bigNumberForQuantity(data);
          }
          return data;
        }
      },
      {
        "targets": "big-size",
        "render": function (data, type) {
          if (type === 'display') {
            if (data === null || data === undefined) {
              return '&mdash;';
            }
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
      },
      {
        "data": "serverIdle"
      },
      {
        "data": "lowMemoryDetected"
      },
      {
        "data": "scansPausedForMemory"
      },
      {
        "data": "scansReturnedEarlyForMemory"
      }
    ]
  });

  refreshScanServers();
});
