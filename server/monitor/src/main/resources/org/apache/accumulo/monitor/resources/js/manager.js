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
    $, document, sessionStorage, getManager, bigNumberForQuantity,
    timeDuration, dateFormat
*/
"use strict";

var managerStatusTable, recoveryListTable;

/**
 * Performs an ajax reload for the given Datatable
 *
 * @param {DataTable} table DataTable to perform an ajax reload on
 */
function ajaxReloadTable(table) {
    if (table) {
        table.ajax.reload(null, false); // user paging is not reset on reload
    } else {
        console.error('Given table could not be found and was not reloaded');
    }
}

/**
 * Populates tables with the new information
 */
function refreshTables() {
    ajaxReloadTable(managerStatusTable);
    ajaxReloadTable(recoveryListTable);
}

/**
 * Used to redraw the page
 */
function refresh() {
    console.log('refreshing');
    refreshTables();
}

/**
 * Creates manager initial table
 * 
 */
$(document).ready(function () {

    // Generates the manager table
    managerStatusTable = $('#managerStatus').DataTable({
        "ajax": {
            "url": '/rest/manager',
            "dataSrc": function (json) {
                // the data needs to be in an array to work with DataTables
                var arr = [json];
                return arr;
            }
        },
        "stateSave": true,
        "columnDefs": [
            {
                "targets": "big-num",
                "render": function (data, type) {
                    if (type === 'display') {
                        data = bigNumberForQuantity(data);
                    }
                    return data;
                }
            },
            {
                "targets": "big-num-rounded",
                "render": function (data, type) {
                    if (type === 'display') {
                        data = bigNumberForQuantity(Math.round(data));
                    }
                    return data;
                }
            },
            {
                "targets": "duration",
                "render": function (data, type) {
                    if (type === 'display') {
                        data = timeDuration(parseInt(data, 10));
                    }
                    return data;
                }
            }
        ],
        "columns": [
            { "data": "manager" },
            { "data": "onlineTabletServers" },
            { "data": "totalTabletServers" },
            {
                "data": "lastGC",
                "type": "html",
                "render": function (data, type) {
                    if (type === 'display') {
                        if (data !== 'Waiting') {
                            data = dateFormat(parseInt(data, 10));
                        }
                        data = '<a href="/gc">' + data + '</a>';
                    }
                    return data;
                }
            },
            { "data": "tablets" },
            { "data": "unassignedTablets" },
            { "data": "numentries" },
            { "data": "ingestrate" },
            { "data": "entriesRead" },
            { "data": "queryrate" },
            { "data": "holdTime" },
            { "data": "osload" },
        ]
    });

    recoveryListTable = $('#recoveryList').DataTable({
        "ajax": {
            "url": '/rest/tservers/recovery',
            "dataSrc": function (data) {
                data = data.recoveryList;
                if (data.length === 0) {
                    console.info('Recovery list is empty, hiding recovery table');
                    $('#recoveryList_wrapper').hide();
                } else {
                    $('#recoveryList_wrapper').show();
                }
                return data;
            }
        },
        "columnDefs": [
            {
                "targets": "duration",
                "render": function (data, type) {
                    if (type === 'display') {
                        data = timeDuration(parseInt(data, 10));
                    }
                    return data;
                }
            },
            {
                "targets": "percent",
                "render": function (data, type) {
                    if (type === 'display') {
                        data = (data * 100).toFixed(2) + '%';
                    }
                    return data;
                }
            }
        ],
        "stateSave": true,
        "columns": [
            { "data": "server" },
            { "data": "log" },
            { "data": "time" },
            { "data": "progress" }
        ]
    });

    refreshTables();
});