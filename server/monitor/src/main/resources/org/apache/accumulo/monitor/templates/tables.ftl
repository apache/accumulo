<#--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
      <script type="text/javascript">
      var tableList;
        /**
         * Creates DataTables table
         *   - uses ajax call for data source and saves sort state in session
         *   - defines custom number formats
         */
        $(document).ready(function() {
          $(document).tooltip();
          tableList = $('#tableList').DataTable( {
                "ajax": {
                  "url": "/rest/tables",
                  "dataSrc": "table"
                },
                "stateSave": true,
                "columnDefs": [
                    { "type": "num",
                      "targets": "big-num",
                      "render": function ( data, type, row ) {
                        if(type === 'display')
                          data = bigNumberForQuantity(data);
                        return data;
                      }
                    }
                  ],
                "columns": [
                    { "data": "tablename",
                      "type": "html",
                      "render": function ( data, type, row, meta ) {
                        if(type === 'display'){
                            data = '<a href="/tables/' + row.tableId + '">' + row.tablename + '</a>';
                        }
                        return data;
                      }
                    },
                    { "data": "tableState" },
                    { "data": "tablets" },
                    { "data": "offlineTablets" },
                    { "data": "recs" },
                    { "data": "recsInMemory" },
                    { "data": "ingestRate" },
                    { "data": "entriesRead" },
                    { "data": "entriesReturned" },
                    { "data": "holdTime" },
                    { "data": "scansCombo" },
                    { "data": "minorCombo" },
                    { "data": "majorCombo" }
                  ]
            } );
        });

        /**
         * Used to refresh the table
         */
        function refresh() {
          <#if js??>
            refreshMaster();
          </#if>

          tableList.ajax.reload();
        }
      </script>
      <div><h3>${tablesTitle}</h3></div>
      <div>
          <table id="tableList" class="table table-bordered table-striped table-condensed">
          <thead>
            <tr><th>Table&nbsp;Name</th>
                <th>State</th>
                <th title="Tables are broken down into ranges of rows called tablets." class="big-num">Tablets</th>
                <th title="Tablets unavailable for query or ingest. May be a transient condition when tablets are moved for balancing." class="big-num">Offline</th>
                <th title="Key/value pairs over each instance, table or tablet." class="big-num">Entries&nbsp</th>
                <th title="The total number of key/value pairs stored in memory and not yet written to disk." class="big-num">In&nbsp;Mem</th>
                <th title="The rate of Key/Value pairs inserted. (Note that deletes are considered inserted)" class="big-num">Ingest</th>
                <th title="The rate of Key/Value pairs read on the server side. Not all key values read may be returned to client because of filtering." class="big-num">Read</th>
                <th title="The rate of Key/Value pairs returned to clients during queries. This is not the number of scans." class="big-num">Returned</th>
                <th title="The amount of time that ingest operations are suspended while waiting for data to be written to disk." class="big-num">Hold&nbsp;Time</th>
                <th title="Running scans. The number queued waiting are in parentheses.">Scans</th>
                <th title="Minor Compactions. The number of tablets waiting for compaction are in parentheses.">MinC</th>
                <th title="Major Compactions. The number of tablets waiting for compaction are in parentheses.">MajC</th></tr>
          </thead>
        </table>
      </div>
