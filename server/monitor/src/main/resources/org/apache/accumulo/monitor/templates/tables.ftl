<#--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
      <script>
        var tableList;
        /**
         * Creates DataTables table
         *   - uses ajax call for data source and saves sort state in session
         *   - defines custom number formats
         */
        $(document).ready(function () {

          tableList = $('#tableList').DataTable({
            "ajax": {
              "url": "/rest-v2/tables",
              "dataSrc": function (json) {
                return Object.keys(json).map(function (key) {
                  json[key].tablename = key;
                  return json[key];
                });
              }
            },
            "stateSave": true,
            "columnDefs": [
              {
                "type": "num",
                "targets": "big-num",
                "render": function (data, type, row) {
                  if (type === 'display')
                    data = bigNumberForQuantity(data);
                  return data;
                }
              }
            ],
            "columns": [
              {
                "data": "tablename",
                "type": "html",
                "render": function (data, type, row, meta) {
                  if (type === 'display') {
                    data = '<a href="/tables/' + row.tablename + '">' + row.tablename + '</a>';
                  }
                  return data;
                }
              },
              { "data": "totalTablets", "orderSequence": ["desc", "asc"] },
              { "data": "totalEntries", "orderSequence": ["desc", "asc"] },
              { "data": "totalSizeOnDisk", "orderSequence": ["desc", "asc"] },
              { "data": "totalFiles", "orderSequence": ["desc", "asc"] },
              { "data": "totalWals", "orderSequence": ["desc", "asc"] },
              { "data": "availableAlways", "orderSequence": ["desc", "asc"] },
              { "data": "availableOnDemand", "orderSequence": ["desc", "asc"] },
              { "data": "availableNever", "orderSequence": ["desc", "asc"] },
              { "data": "totalAssignedTablets", "orderSequence": ["desc", "asc"] },
              { "data": "totalAssignedToDeadServerTablets", "orderSequence": ["desc", "asc"] },
              { "data": "totalHostedTablets", "orderSequence": ["desc", "asc"] },
              { "data": "totalSuspendedTablets", "orderSequence": ["desc", "asc"] },
              { "data": "totalUnassignedTablets", "orderSequence": ["desc", "asc"] }
            ]
          });
        });

        /**
         * Used to refresh the table
         */
        function refresh() {
          <#if js??>
            refreshManagerTables();
          </#if>

          tableList.ajax.reload(null, false); // user paging is not reset on reload
        }
      </script>
      <div class="row">
        <div class="col-xs-12">
          <h3>Table Overview</h3>
        </div>
      </div>
      <div>
        <table id="tableList" class="table caption-top table-bordered table-striped table-condensed">
          <caption><span class="table-caption">${tablesTitle}</span><br />
          <thead>
            <tr>
              <th title="Table Name">Table&nbsp;Name</th>
              <th title="Tables are broken down into ranges of rows called tablets." class="big-num">Tablets</th>
              <th title="Key/value pairs over each instance, table or tablet." class="big-num">Entries</th>
              <th title="Total size on disk." class="big-num">Size on Disk</th>
              <th title="Total number of files." class="big-num">Files</th>
              <th title="Total number of WALs." class="big-num">WALs</th>
              <th title="Number of tablets that are always hosted." class="big-num">Always Available</th>
              <th title="Number of tablets that are hosted on demand." class="big-num">On Demand</th>
              <th title="Number of tablets that are never hosted." class="big-num">Never Available</th>
              <th title="Total assigned tablets." class="big-num">Assigned Tablets</th>
              <th title="Tablets assigned to dead servers." class="big-num">Assigned to Dead Servers</th>
              <th title="Number of tablets that are currently hosted." class="big-num">Hosted Tablets</th>
              <th title="Number of suspended tablets." class="big-num">Suspended Tablets</th>
              <th title="Number of unassigned tablets." class="big-num">Unassigned Tablets</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
