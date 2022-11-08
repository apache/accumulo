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

var coordinatorTable;
var compactorsTable;
var compactorsTableData;
var runningTable;
var runningTableData;

/**
 * Creates active compactions table
 */
$(document).ready(function () {
  if (sessionStorage.ecDetailsJSON === undefined) {
    sessionStorage.ecDetailsJSON = JSON.stringify([]);
  }

  // display datatables errors in the console instead of in alerts
  $.fn.dataTable.ext.errMode = 'throw';

  compactorsTable = $('#compactorsTable').DataTable({
    "ajax": {
      "url": '/rest/ec/compactors',
      "dataSrc": "compactors"
    },
    "stateSave": true,
    "dom": 't<"align-left"l>p',
    "columnDefs": [{
        "targets": "duration",
        "render": function (data, type, row) {
          if (type === 'display') data = timeDuration(data);
          return data;
        }
      },
      {
        "targets": "date",
        "render": function (data, type, row) {
          if (type === 'display') data = dateFormat(data);
          return data;
        }
      }
    ],
    "columns": [{
        "data": "server"
      },
      {
        "data": "queueName"
      },
      {
        "data": "lastContact"
      }
    ]
  });

  // Create a table for running compactors
  runningTable = $('#runningTable').DataTable({
    "ajax": {
      "url": '/rest/ec/running',
      "dataSrc": "running"
    },
    "stateSave": true,
    "dom": 't<"align-left"l>p',
    "columnDefs": [{
        "targets": "duration",
        "render": function (data, type, row) {
          if (type === 'display') data = timeDuration(data);
          return data;
        }
      },
      {
        "targets": "date",
        "render": function (data, type, row) {
          if (type === 'display') data = dateFormat(data);
          return data;
        }
      }
    ],
    "columns": [{
        "data": "server"
      },
      {
        "data": "kind"
      },
      {
        "data": "status"
      },
      {
        "data": "queueName"
      },
      {
        "data": "tableId"
      },
      {
        "data": "numFiles"
      },
      {
        "data": "progress",
        "type": "html",
        "render": function (data, type, row, meta) {
          if (type === 'display') {
            if (row.progress < 0) {
              data = '--';
            } else {
              var p = Math.round(Number(row.progress));
              console.log("Compaction progress = %" + p);
              data = '<div class="progress"><div class="progress-bar" role="progressbar" style="min-width: 2em; width:' +
                p + '%;">' + p + '%</div></div>';
            }
          }
          return data;
        }
      },
      {
        "data": "lastUpdate"
      },
      {
        "data": "duration"
      },
      { // more column settings
        "class": "details-control",
        "orderable": false,
        "data": null,
        "defaultContent": ""
      }
    ]
  });

  // Create a table for compaction coordinator
  coordinatorTable = $('#coordinatorTable').DataTable({
    "ajax": {
      "url": '/rest/ec',
      "dataSrc": function (data) {
        // the data needs to be in an array to work with DataTables
        var arr = [];
        if (data === undefined) {
          console.warn('the value of "data" is undefined');
        } else {
          arr = [data];
        }

        return arr;
      }
    },
    "stateSave": true,
    "searching": false,
    "paging": false,
    "info": false,
    "columnDefs": [{
      "targets": "duration",
      "render": function (data, type, row) {
        if (type === 'display') data = timeDuration(data);
        return data;
      }
    }],
    "columns": [{
        "data": "server"
      },
      {
        "data": "numQueues"
      },
      {
        "data": "numCompactors"
      },
      {
        "data": "lastContact"
      }
    ]
  });

  // Array to track the ids of the details displayed rows
  var detailRows = [];
  $("#runningTable tbody").on('click', 'tr td.details-control', function () {
    var tr = $(this).closest('tr');
    var row = runningTable.row(tr);
    var idx = $.inArray(tr.attr('id'), detailRows);

    if (row.child.isShown()) {
      tr.removeClass('details');
      row.child.hide();

      // Remove from the 'open' array
      detailRows.splice(idx, 1);
    } else {
      var rci = row.data();
      var ecid = rci.ecid;
      var idSuffix = ecid.substring(ecid.length - 5, ecid.length);
      tr.addClass('details');
      // put all the information into html for a single row
      var htmlRow = "<table class='table table-bordered table-striped table-condensed' id='table" + idSuffix + "'>"
      htmlRow += "<thead><tr><th>#</th><th>Input Files</th><th>Size</th><th>Entries</th></tr></thead>";
      htmlRow += "<tbody></tbody></table>";
      htmlRow += "Output File: <span id='outputFile" + idSuffix + "'></span><br>";
      htmlRow += ecid;
      row.child(htmlRow).show();
      // show the row then populate the table
      var ecDetails = getDetailsFromStorage(idSuffix);
      if (ecDetails.length === 0) {
        getRunningDetails(ecid, idSuffix);
      } else {
        console.log("Got cached details for " + idSuffix);
        populateDetails(ecDetails, idSuffix);
      }

      // Add to the 'open' array
      if (idx === -1) {
        detailRows.push(tr.attr('id'));
      }
    }
  });
  refreshECTables();
});

/**
 * Used to redraw the page
 */
function refresh() {
  refreshECTables();
}

/**
 * Refreshes the compaction tables
 */
function refreshECTables() {
  // user paging is not reset on reload
  ajaxReloadTable(compactorsTable);
  ajaxReloadTable(runningTable);
  ajaxReloadTable(coordinatorTable);
}


function getRunningDetails(ecid, idSuffix) {
  var ajaxUrl = '/rest/ec/details?ecid=' + ecid;
  console.log("Ajax call to " + ajaxUrl);
  $.getJSON(ajaxUrl, function (data) {
    populateDetails(data, idSuffix);
    var detailsJSON = JSON.parse(sessionStorage.ecDetailsJSON);
    if (detailsJSON === undefined) {
      detailsJSON = [];
    } else if (detailsJSON.length >= 50) {
      // drop the oldest 25 from the sessionStorage to limit size of the cache
      var newDetailsJSON = [];
      $.each(detailsJSON, function (num, val) {
        if (num > 24) {
          newDetailsJSON.push(val);
        }
      });
      detailsJSON = newDetailsJSON;
    }
    detailsJSON.push({
      key: idSuffix,
      value: data
    });
    sessionStorage.ecDetailsJSON = JSON.stringify(detailsJSON);
  });
}

function getDetailsFromStorage(idSuffix) {
  var details = [];
  var detailsJSON = JSON.parse(sessionStorage.ecDetailsJSON);
  if (detailsJSON.length === 0) {
    return details;
  } else {
    // details are stored as key value pairs in the JSON val
    $.each(detailsJSON, function (num, val) {
      if (val.key === idSuffix) {
        details = val.value;
      }
    });
    return details;
  }
}

function populateDetails(data, idSuffix) {
  var tableId = 'table' + idSuffix;
  clearTableBody(tableId);
  $.each(data.inputFiles, function (key, value) {
    var items = [];
    items.push(createCenterCell(key, key));
    items.push(createCenterCell(value.metadataFileEntry, value.metadataFileEntry));
    items.push(createCenterCell(value.size, bigNumberForSize(value.size)));
    items.push(createCenterCell(value.entries, bigNumberForQuantity(value.entries)));
    $('<tr/>', {
      html: items.join('')
    }).appendTo('#' + tableId + ' tbody');
  });
  $('#outputFile' + idSuffix).text(data.outputFile);
}

function refreshCompactors() {
  console.log("Refresh compactors table.");
  // user paging is not reset on reload
  ajaxReloadTable(compactorsTable);
}

function refreshRunning() {
  console.log("Refresh running compactions table.");
  // user paging is not reset on reload
  ajaxReloadTable(runningTable);
}
