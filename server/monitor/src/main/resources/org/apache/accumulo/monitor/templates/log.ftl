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
              { "data": "logger" },
              { "data": "count" },
              { "data": "level",
                "type": "html",
                "render": function ( data, type, row, meta ) {
                  if(type === 'display') data = levelFormat(row.level);
                  return data;
                }
              },
              { "data": "message" },
              {
                "class":          "details-control",
                "orderable":      false,
                "data":           null,
                "defaultContent": ""
              }
            ]
          });
          // Array to track the ids of the details displayed rows
          var detailRows = [];

          $("#logTable tbody").on( 'click', 'tr td.details-control', function () {
            var tr = $(this).closest('tr');
            var row = logList.row( tr );
            var idx = $.inArray( tr.attr('id'), detailRows );

            if ( row.child.isShown() ) {
                tr.removeClass( 'details' );
                row.child.hide();

                // Remove from the 'open' array
                detailRows.splice( idx, 1 );
            }
            else {
                tr.addClass( 'details' );
                row.child( "<pre>" + row.data().stacktrace + "</pre>" ).show();

                // Add to the 'open' array
                if ( idx === -1 ) {
                    detailRows.push( tr.attr('id') );
                }
            }
          });

          logList.on( 'draw', function () {
              // remove the details button for rows without a stacktrace
              $("#logTable tr").each(function( i, element) {
                var r = logList.row(element).data();
                if (r && r.stacktrace == null) {
                  var c = $(element).find('td:last-child');
                  c.removeClass('details-control');
                }
              });
              // On each draw, loop over the `detailRows` array and show any child rows
              $.each( detailRows, function ( i, id ) {
                  $('#'+id+' td.details-control').trigger( 'click' );
              });
          });
        }); // end document ready

        /**
         * Used to refresh the table
         */
        function refresh() {
          logList.ajax.reload(null, false ); // user paging is not reset on reload
          refreshNavBar();
        }
      </script>
      <div><h3>${title}</h3></div>
        <table id="logTable" class="table caption-top table-bordered table-striped table-condensed">
         <caption><span class="table-caption">Logs</span><br /></caption>
          <thead>
            <tr>
              <th>Timestamp</th>
              <th>Application</th>
              <th>Logger</th>
              <th>Count</th>
              <th>Level</th>
              <th class="logevent">Message</th>
              <th>Stacktrace</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      <div>
       <button type="button" class="btn btn-white-font" onclick="clearLogs();">Clear Logs</button>
      </div>
