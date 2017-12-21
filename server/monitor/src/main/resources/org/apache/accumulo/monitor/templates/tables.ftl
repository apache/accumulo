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
         *   - uses scientific notation plugin for sorting and ellipsis plugin to shorten numbers
         */
        $(document).ready(function() {
          $(document).tooltip();
          tableList = $('#tableList').DataTable( {
                "ajaxSource": "/rest/dataTables",
                "stateSave": true,
                "columnDefs": [
                    { "type": "scientific",
                      "targets": "sci-notation",
                      "render": $.fn.dataTable.render.ellipsis( 6, true )
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
                      }},
                    { "data": "tableState" },
                    { "data": "tablets", "type": "num" },
                    { "data": "offlineTablets", "type": "num" },
                    { "data": "recs", "type": "num" },
                    { "data": "recsInMemory", "type": "num" },
                    { "data": "ingest" },
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
          tableList.ajax.reload();
        }
      </script>
      <div><h3>${tablesTitle}</h3></div>
      <div id="tablesBanner"></div>
      <div class="center-block">
          <table id="tableList" class="table table-bordered table-striped table-condensed">
          <thead>
            <tr><th>Table&nbsp;Name&nbsp;</th>
                <th>State&nbsp;</th>
                <th title="Tables are broken down into ranges of rows called tablets.">#&nbsp;Tablets</th>
                <th title="Tablets unavailable for query or ingest. May be a transient condition when tablets are moved for balancing.">Offline</th>
                <th title="Key/value pairs over each instance, table or tablet.">Entries&nbsp;</th>
                <th title="The total number of key/value pairs stored in memory and not yet written to disk."><br/>In&nbsp;Mem</th>
                <th title="The number of Key/Value pairs inserted. (Note that deletes are considered inserted)" class="sci-notation">Ingest&nbsp;</th>
                <th title="The number of Key/Value pairs read on the server side. Not all key values read may be returned to client because of filtering." class="sci-notation">Read</th>
                <th title="The number of Key/Value pairs returned to clients during queries. This is not the number of scans." class="sci-notation">Returned</th>
                <th title="The amount of time that ingest operations are suspended while waiting for data to be written to disk." class="sci-notation">Hold&nbsp;Time</th>
                <th title="Running scans. The number queued waiting are in parentheses.">Scans</th>
                <th title="Minor Compactions. The number of tablets waiting for compaction are in parentheses.">MinC</th>
                <th title="Major Compactions. The number of tablets waiting for compaction are in parentheses.">MajC</th></tr>
          </thead>
        </table>
      </div>
