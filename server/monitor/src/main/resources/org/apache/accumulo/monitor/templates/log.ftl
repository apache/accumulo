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
      var logList;
        /**
         * Creates DataTables table
         *   - uses ajax call for data source and saves sort state in session
         *   - defines custom number formats
         */
        $(document).ready(function() {
          logList = $('#logTable').DataTable( {
            "ajax": {
              "url": '/rest/logs',
              "dataSrc": ""
            },
            "stateSave": true,
            "columns": [
              { "data": "timestamp",
                "type": "html",
                "render": function ( data, type, row, meta ) {
                  if(type === 'display') data = dateFormat(row.timestamp);
                  return data;
                }
              },
              { "data": "application" },
              { "data": "count" },
              { "data": "level",
                "type": "html",
                "render": function ( data, type, row, meta ) {
                  if(type === 'display') data = levelFormat(row.level);
                  return data;
                }
              },
              { "data": "message" }
            ]
          });
        });

        /**
         * Used to refresh the table
         */
        function refresh() {
          logList.ajax.reload();
        }
      </script>
      <div><h3>${title}</h3></div>
      <div>
        <table id="logTable" class="table table-bordered table-striped table-condensed">
            <thead><tr>
              <th>Timestamp</th>
              <th>Application</th>
              <th>Count</th>
              <th>Level</th>
              <th class="logevent">Message</th></tr>
            </thead>
            <tbody></tbody>
        </table>
      </div>
