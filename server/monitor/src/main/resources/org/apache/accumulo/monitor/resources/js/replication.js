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

var replicationStatsTable;

/**
 * Populates the table with the new information
 */
function refreshReplication() {
  ajaxReloadTable(replicationStatsTable);
}

/**
 * Used to redraw the page
 */
function refresh() {
  refreshReplication();
}

/**
 * Creates replication initial table
 */
$(document).ready(function () {

  replicationStatsTable = $('#replicationStats').DataTable({
    "ajax": {
      "url": "/rest/replication",
      "dataSrc": ""
    },
    "stateSave": true,
    "columns": [{
        "data": "tableName",
        "width": "25%"
      },
      {
        "data": "peerName",
        "width": "20%"
      },
      {
        "data": "remoteIdentifier",
        "width": "25%"
      },
      {
        "data": "replicaSystemType",
        "width": "15%"
      },
      {
        "data": "filesNeedingReplication",
        "width": "15%",
        "render": function (data, type) {
          if (type === 'display') {
            data = bigNumberForQuantity(data);
          }
          return data;
        }
      }
    ]
  });

  refreshReplication();

});
