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
 * Creates overview initial tables
 */
$(document).ready(function() {
  refreshOverview();
});

/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshOverview() {
  getMaster().then(function() {
    refreshMasterTable();
  });
  getZK().then(function() {
    refreshZKTable();
  });
  var requests = [
    getIngestRate(),
    getScanEntries(),
    getIngestByteRate(),
    getQueryByteRate(),
    getLoadAverage(),
    getLookups(),
    getMinorCompactions(),
    getMajorCompactions(),
    getIndexCacheHitRate(),
    getDataCacheHitRate()
  ];
  $.when(...requests).always(function() {
    makePlots();
  });
}

/**
 * Used to redraw the page
 */
function refresh() {
  refreshOverview();
}

/**
 * Refreshes the master table
 */
function refreshMasterTable() {
  var data = sessionStorage.master === undefined ?
      [] : JSON.parse(sessionStorage.master);

  $('#master tr td:first').hide();
  $('#master tr td').hide();

  // If the master is down, show the first row, otherwise refresh old values
  if (data.length === 0 || data.master === 'No Masters running') {
    $('#master tr td:first').show();
  } else {
    $('#master tr td:not(:first)').show();
    var table = $('#master td.right');

    table.eq(0).html(bigNumberForQuantity(data.tables));
    table.eq(1).html(bigNumberForQuantity(data.totalTabletServers));
    table.eq(2).html(bigNumberForQuantity(data.deadTabletServersCount));
    table.eq(3).html(bigNumberForQuantity(data.tablets));
    table.eq(4).html(bigNumberForQuantity(data.numentries));
    table.eq(5).html(bigNumberForQuantity(data.lookups));
    table.eq(6).html(timeDuration(data.uptime));
  }
}

/**
 * Refresh the zookeeper table
 */
function refreshZKTable() {
  var data = sessionStorage.zk === undefined ?
      [] : JSON.parse(sessionStorage.zk);

  $('#zookeeper thead tr:last').hide();
  clearTableBody('zookeeper');

  if (data.length === 0 || data.zkServers.length === 0) {
    $('#zookeeper thead tr:last').show();
  } else {
    $.each(data.zkServers, function(key, val) {
      var cells = '<td class="left">' + val.server + '</td>';
      if (val.clients >= 0) {
        cells += '<td class="left">' + val.mode + '</td>';
        cells += '<td class="right">' + val.clients + '</td>';
      } else {
        cells += '<td class="left"><span class="error">Down</span></td>';
        cells += '<td class="right"></td>';
      }
      // create a <tr> element with html containing the cell data; append it to the table
      $('<tr/>', { html: cells }).appendTo('#zookeeper tbody');
    });
  }
}

//// Overview plot creation

/**
 * Create the plots for the overview page
 */
var plotOptions = {
  colors: ['#d9534f', '#337ab7'],
  grid: {
    backgroundColor: {colors: ['#fff', '#eee']}
  },
  lines: {
    show: true
  },
  points: {
    show: false,
    radius: 1
  },
  xaxis: {
    mode: 'time',
    minTickSize: [1, 'minute'],
    timeformat: '%H:%M',
    ticks: 3
  },
  yaxis: {
    min: 0
  }
};

var cachePlotOptions = $.extend(true, {}, plotOptions, {
  lines: { show: false },
  points: { show: true },
  yaxis: {
    max: 1.1,
    ticks: [0, 0.25, 0.5, 0.75, 1.0]
  }
});

function makePlots() {
  var d = new Date();
  var n = d.getTimezoneOffset() * 60000; // Converts offset to milliseconds

  // Create Ingest Rate plot
  var ingestRate = [{data:[]}];
  var data = sessionStorage.ingestRate === undefined ?
      [] : JSON.parse(sessionStorage.ingestRate);
  $.each(data, function(key, val) {
    ingestRate[0].data.push([val.first - n, val.second]);
  });
  $.plot('#ingest_entries', ingestRate, plotOptions);

  // Create Scan Entries plot
  var scanEntries = [
    {label: 'Read', data: []},
    {label: 'Returned', data: []}
  ];
  data = sessionStorage.scanEntries === undefined ?
      [] : JSON.parse(sessionStorage.scanEntries);
  $.each(data[0].second, function(key, val) {
    scanEntries[0].data.push([val.first - n, val.second]);
  });
  $.each(data[1].second, function(key, val) {
    scanEntries[1].data.push([val.first - n, val.second]);
  });
  $.plot('#scan_entries', scanEntries, plotOptions);

  // Create Ingest MB plot
  var ingestMB = [{data:[]}];
  data = sessionStorage.ingestMB === undefined ?
      [] : JSON.parse(sessionStorage.ingestMB);
  $.each(data, function(key, val) {
    ingestMB[0].data.push([val.first - n, val.second]);
  });
  $.plot('#ingest_mb', ingestMB, plotOptions);

  // Create Query MB plot
  var queryMB = [{data:[]}];
  data = sessionStorage.queryMB === undefined ?
      [] : JSON.parse(sessionStorage.queryMB);
  $.each(data, function(key, val) {
    queryMB[0].data.push([val.first - n, val.second]);
  });
  $.plot('#scan_mb', queryMB, plotOptions);

  // Create Load Average plot
  var loadAvg = [{data:[]}];
  data = sessionStorage.loadAvg === undefined ?
      [] : JSON.parse(sessionStorage.loadAvg);
  $.each(data, function(key, val) {
    loadAvg[0].data.push([val.first - n, val.second]);
  });
  $.plot('#load_avg', loadAvg, plotOptions);

  // Create Seeks plot
  var lookups = [{data:[]}];
  data = sessionStorage.lookups === undefined ?
      [] : JSON.parse(sessionStorage.lookups);
  $.each(data, function(key, val) {
    lookups[0].data.push([val.first - n, val.second]);
  });
  $.plot('#seeks', lookups, plotOptions);

  // Create Minor Compactions plot
  var minor = [{data:[]}];
  data = sessionStorage.minorCompactions === undefined ?
      [] : JSON.parse(sessionStorage.minorCompactions);
  $.each(data, function(key, val) {
    minor[0].data.push([val.first - n, val.second]);
  });
  $.plot('#minor', minor, plotOptions);

  // Create Major Compaction plot
  var major = [{data:[]}];
  data = sessionStorage.majorCompactions === undefined ?
      [] : JSON.parse(sessionStorage.majorCompactions);
  $.each(data, function(key, val) {
    major[0].data.push([val.first - n, val.second]);
  });
  $.plot('#major', major, plotOptions);

  // Create Index Cache plot
  var indexCache = [{data:[]}];
  data = sessionStorage.indexCache === undefined ?
      [] : JSON.parse(sessionStorage.indexCache);
  $.each(data, function(key, val) {
    indexCache[0].data.push([val.first - n, val.second]);
  });
  $.plot('#index_cache', indexCache, cachePlotOptions);

  // Create Data Cache plot
  var dataCache = [{data:[]}];
  data = sessionStorage.dataCache === undefined ?
      [] : JSON.parse(sessionStorage.dataCache);
  $.each(data, function(key, val) {
    dataCache[0].data.push([val.first - n, val.second]);
  });
  $.plot('#data_cache', dataCache, cachePlotOptions);
}
