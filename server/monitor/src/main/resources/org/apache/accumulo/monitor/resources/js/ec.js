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
$(function () {
  if (sessionStorage.ecDetailsJSON === undefined) {
    sessionStorage.ecDetailsJSON = JSON.stringify([]);
  }

  // display datatables errors in the console instead of in alerts
  $.fn.dataTable.ext.errMode = 'throw';

  compactorsTable = $('#compactorsTable').DataTable({
    "autoWidth": false,
    "ajax": {
      "url": contextPath + 'rest/ec/compactors',
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
        "data": "groupName"
      },
      {
        "data": "lastContact"
      }
    ]
  });

  const hostnameColumnName = 'hostname';
  const queueNameColumnName = 'queueName';
  const tableIdColumnName = 'tableId';
  const ecidColumnName = 'ecid';
  const durationColumnName = 'duration';

  // Create a table for running compactors
  runningTable = $('#runningTable').DataTable({
    "autoWidth": false,
    "ajax": {
      "url": contextPath + 'rest/ec/running',
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
        "data": "server",
        "name": hostnameColumnName
      },
      {
        "data": "kind"
      },
      {
        "data": "status"
      },
      {
        "data": "queueName",
        "name": queueNameColumnName
      },
      {
        "data": "tableId",
        "name": tableIdColumnName
      },
      {
        "data": "ecid",
        "name": ecidColumnName
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
        "data": "duration",
        "name": durationColumnName
      },
      { // more column settings
        "class": "details-control",
        "orderable": false,
        "data": null,
        "defaultContent": ""
      }
    ]
  });

  function handleFilterKeyup(input, feedbackElement, columnName) {
    if (isValidRegex(input) || input === '') { // if valid, apply the filter
      feedbackElement.hide();
      $(this).removeClass('is-invalid');
      const isRegex = true;
      const smartEnabled = false;
      runningTable
        .column(`${columnName}:name`)
        .search(input, isRegex, smartEnabled)
        .draw();
    } else { // if invalid, show the warning
      feedbackElement.show();
      $(this).addClass('is-invalid');
    }
  }

  $('#hostname-filter').on('keyup', function () {
    handleFilterKeyup.call(this, this.value, $('#hostname-feedback'), hostnameColumnName);
  });

  $('#queue-filter').on('keyup', function () {
    handleFilterKeyup.call(this, this.value, $('#queue-feedback'), queueNameColumnName);
  });

  $('#tableid-filter').on('keyup', function () {
    handleFilterKeyup.call(this, this.value, $('#tableid-feedback'), tableIdColumnName);
  });

  $('#ecid-filter').on('keyup', function () {
    handleFilterKeyup.call(this, this.value, $('#ecid-feedback'), ecidColumnName);
  });

  $('#duration-filter').on('keyup', function () {
    runningTable.draw();
  });

  // Clear Filters button handler
  $('#clear-filters').on('click', function () {
    $(this).prop('disabled', true); // disable the clear button

    // set the filter inputs to empty and trigger the keyup event to clear the filters
    $('#hostname-filter').val('').trigger('keyup');
    $('#queue-filter').val('').trigger('keyup');
    $('#tableid-filter').val('').trigger('keyup');
    $('#ecid-filter').val('').trigger('keyup');
    $('#duration-filter').val('').trigger('keyup');

    $(this).prop('disabled', false); // re-enable the clear
  });

  // Custom filter function for duration
  $.fn.dataTable.ext.search.push(function (settings, data, dataIndex) {
    if (settings.nTable.id !== 'runningTable') {
      return true;
    }

    const durationColIndex = runningTable.column(`${durationColumnName}:name`).index();
    const durationStr = data[durationColIndex];
    const durationSeconds = parseDuration(durationStr);

    const input = $('#duration-filter').val().trim();
    if (input === '') {
      $('#duration-feedback').hide();
      return true;
    }

    const match = validateDurationInput(input);
    if (!match) {
      $('#duration-feedback').show();
      return false;
    }

    $('#duration-feedback').hide();
    const operator = match[1];
    const value = parseInt(match[2]);
    const unit = match[3];
    const filterSeconds = convertToSeconds(value, unit);

    switch (operator) {
    case '>':
      return durationSeconds > filterSeconds;
    case '>=':
      return durationSeconds >= filterSeconds;
    case '<':
      return durationSeconds < filterSeconds;
    case '<=':
      return durationSeconds <= filterSeconds;
    default:
      console.error(`Unexpected operator "${operator}" encountered in duration filter.`);
      return true;
    }
  });

  // Helper function to convert duration strings to seconds
  function convertToSeconds(value, unit) {
    switch (unit.toLowerCase()) {
    case 's':
      return value;
    case 'm':
      return value * 60;
    case 'h':
      return value * 3600;
    case 'd':
      return value * 86400;
    default:
      console.error(`Unexpected unit "${unit}" encountered in duration filter. Defaulting to seconds.`);
      return value;
    }
  }

  // Helper function to validate duration input. Makes sure that the input is in the format of '<operator> <value> <unit>'
  function validateDurationInput(input) {
    return input.match(/^([<>]=?)\s*(\d+)([smhd])$/i);
  }

  /**
   * @param {number} durationStr duration in milliseconds
   * @returns duration in seconds
   */
  function parseDuration(durationStr) {
    // Assuming durationStr is in milliseconds
    const milliseconds = parseInt(durationStr, 10);
    const seconds = milliseconds / 1000;
    return seconds;
  }

  // Create a table for compaction coordinator
  coordinatorTable = $('#coordinatorTable').DataTable({
    "autoWidth": false,
    "ajax": {
      "url": contextPath + 'rest/ec',
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
  refreshCoordinatorStatus().then(function (coordinatorStatus) {

    // tables will not be shown, avoid reloading
    if (coordinatorStatus === 'ERROR') {
      return;
    }

    // user paging is not reset on reload
    refreshCompactors();
    refreshRunning();
    ajaxReloadTable(coordinatorTable);
  });
}

/**
 * Updates session storage then checks if the coordinator is running. If it is,
 * show the tables and hide the 'coordinator not running' banner. Else, vise-versa.
 *
 * returns the coordinator status
 */
async function refreshCoordinatorStatus() {
  return getStatus().then(function () {
    var coordinatorStatus = JSON.parse(sessionStorage.status).coordinatorStatus;
    if (coordinatorStatus === 'ERROR') {
      // show banner and hide tables
      $('#ccBanner').show();
      $('#ecDiv').hide();
    } else {
      // otherwise, hide banner and show tables
      $('#ccBanner').hide();
      $('#ecDiv').show();
    }
    return coordinatorStatus;
  });
}

function getRunningDetails(ecid, idSuffix) {
  var ajaxUrl = contextPath + 'rest/ec/details?ecid=' + ecid;
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

// Helper function to validate regex
function isValidRegex(input) {
  try {
    new RegExp(input);
    return true;
  } catch (e) {
    return false;
  }
}
