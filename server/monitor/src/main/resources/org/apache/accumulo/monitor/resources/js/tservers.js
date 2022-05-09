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
/* JSLint global definitions */
/*global
    $, document, sessionStorage, getTServers, clearDeadServers, refreshNavBar, getRecoveryList, bigNumberForQuantity, timeDuration
*/
"use strict";

var tserversTable, deadTServersTable, badTServersTable;
var recoveryList = [];

/**
 * Refreshes the list of recovering tservers used to highlight rows
 */
function refreshRecoveryList() {
    $('#recovery-caption').hide(); // Hide the caption about highlighted rows on each refresh
    getRecoveryList().then(function () {
        recoveryList = [];
        var data = sessionStorage.recoveryList === undefined ?
                    [] : JSON.parse(sessionStorage.recoveryList);
        data.recoveryList.forEach(function (entry) {
            recoveryList.push(entry.server);
        });
    });
}

/**
 * Performs an ajax reload for the given Datatable
 */
function ajaxReloadTable(table) {
    if (table) {
        table.ajax.reload(null, false); // user paging is not reset on reload
    }
}

/**
 * Generates the tserver table
 */
function refreshTServersTable() {
    refreshRecoveryList();
    ajaxReloadTable(tserversTable);
}

/**
 * Generates the deadtservers table
 */
function refreshDeadTServersTable() {
    ajaxReloadTable(deadTServersTable);

    if ($('#deadtservers tbody .dataTables_empty').length) {
        $('#deadtservers_wrapper').hide();
    } else {
        $('#deadtservers_wrapper').show();
    }
}

/**
 * Generates the badtservers table
 */
function refreshBadTServersTable() {
    ajaxReloadTable(badTServersTable);

    if ($('#badtservers tbody .dataTables_empty').length) {
        $('#badtservers_wrapper').hide();
    } else {
        $('#badtservers_wrapper').show();
    }
}

/**
 * Makes the REST calls, generates the tables with the new information
 */
function refreshTServers() {
    getTServers().then(function () {
        refreshBadTServersTable();
        refreshDeadTServersTable();
        refreshTServersTable();
    });
}

/**
 * Used to redraw the page
 */
function refresh() {
    refreshTServers();
}

/**
 * Makes the REST POST call to clear dead table server
 *
 * @param {string} server Dead TServer to clear
 */
function clearDeadTServers(server) {
    clearDeadServers(server);
    refreshTServers();
    refreshNavBar();
}

/**
 * Creates tservers initial table
 */
$(document).ready(function () {

    refreshRecoveryList();

    // Create a table for tserver list
    tserversTable = $('#tservers').DataTable({
        "ajax": {
            "url": '/rest/tservers',
            "dataSrc": "servers"
        },
        "stateSave": true,
        "columnDefs": [
            {
                "targets": "big-num",
                "render": function (data, type, row) {
                    if (type === 'display') {
                        data = bigNumberForQuantity(data);
                    }
                    return data;
                }
            },
            {
                "targets": "duration",
                "render": function (data, type, row) {
                    if (type === 'display') data = timeDuration(data);
                    return data;
                }
            },
            {
                "targets": "percent",
                "render": function (data, type, row) {
                    if (type === 'display') data = Math.round(data * 100) + '%';
                    return data;
                }
            }
        ],
        "columns": [
            {
                "data": "hostname",
                "type": "html",
                "render": function (data, type, row, meta) {
                    if (type === 'display') data = '<a href="/tservers?s=' + row.id + '">' + row.hostname + '</a>';
                    return data;
                }
            },
            { "data": "tablets" },
            { "data": "lastContact" },
            { "data": "responseTime" },
            { "data": "entries" },
            { "data": "ingest" },
            { "data": "query" },
            { "data": "holdtime" },
            { "data": "scansCombo" },
            { "data": "minorCombo" },
            { "data": "majorCombo" },
            { "data": "indexCacheHitRate" },
            { "data": "dataCacheHitRate" },
            { "data": "osload" }
        ],
        "rowCallback": function (row, data, index) {
            // reset background of each row
            $(row).css('background-color', '');

            // return if the current row's tserver is not recovering
            if (!recoveryList.includes(data.hostname))
                return;

            // only show the caption if we know there are rows in the tservers table
            $('#recovery-caption').show();

            // highlight current row
            console.log('Highlighting row index:' + index + ' tserver:' + data.hostname);
            $(row).css('background-color', 'gold');
        }
    });

    deadTServersTable = $('#deadtservers').DataTable({
        "ajax": {
            "url": '/rest/tservers',
            "dataSrc": "deadServers"
        },
        "stateSave": true,
        "columnDefs": [
            {
                "targets": "date",
                "render": function (data, type, row) {
                    if (type === 'display' && data > 0) data = dateFormat(data);
                    return data;
                }
            }
        ],
        "columns": [
            { "data": "server" },
            { "data": "lastStatus" },
            { "data": "status" },
            {
                "data": "server",
                "type": "html",
                "render": function (data, type, row, meta) {
                    if (type === 'display') data = `<a href="javascript:clearDeadTServers('${data}');">clear</a>`;
                    return data;
                }
            }
        ]
    });

    badTServersTable = $('#badtservers').DataTable({
        "ajax": {
            "url": '/rest/tservers',
            "dataSrc": "badServers"
        },
        "stateSave": true,
        "columnDefs": [
            {
                "targets": "date",
                "render": function (data, type, row) {
                    if (type === 'display' && data > 0) data = dateFormat(data);
                    return data;
                }
            }
        ],
        "columns": [
            { "data": "id" },
            { "data": "status" }
        ]
    });

    refreshTServers();
});