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

 var coordinatorTable;
 var compactorsTable;
 var compactorsTableData;
 var runningTable;
 var runningTableData;

 /**
  * Creates active compactions table
  */
 $(document).ready(function() {
    compactorsTable = $('#compactorsTable').DataTable({
           "ajax": {
               "url": '/rest/ec/compactors',
               "dataSrc": "compactors"
           },
           "stateSave": true,
           "dom": 't<"align-left"l>p',
           "columnDefs": [
              { "targets": "duration",
                "render": function ( data, type, row ) {
                  if(type === 'display') data = timeDuration(data);
                  return data;
                }
              },
              { "targets": "date",
                  "render": function ( data, type, row ) {
                    if(type === 'display') data = dateFormat(data);
                    return data;
                  }
                }
            ],
           "columns": [
             { "data": "server" },
             { "data": "queueName"},
             { "data": "lastContact"}
           ]
         });

     // Create a table for compactors
     runningTable = $('#runningTable').DataTable({
       "ajax": {
            "url": '/rest/ec/running',
            "dataSrc": "running"
       },
       "stateSave": true,
       "dom": 't<"align-left"l>p',
       "columnDefs": [
           { "targets": "duration",
             "render": function ( data, type, row ) {
               if(type === 'display') data = timeDuration(data);
               return data;
             }
           },
           { "targets": "date",
               "render": function ( data, type, row ) {
                 if(type === 'display') data = dateFormat(data);
                 return data;
               }
             }
         ],
       "columns": [
         { "data": "server" },
         { "data": "kind" },
         { "data": "status" },
         { "data": "queueName" },
         { "data": "tableId" },
         { "data": "numFiles" },
         { "data": "progress",
           "type": "html",
           "render": function ( data, type, row, meta ) {
              if(type === 'display') {
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
         { "data": "lastUpdate"},
         { "data": "duration"},
         { // more column settings
            "class":          "details-control",
            "orderable":      false,
            "data":           null,
            "defaultContent": ""
         }
       ]
     });

     // Array to track the ids of the details displayed rows
       var detailRows = [];
       $("#runningTable tbody").on( 'click', 'tr td.details-control', function () {
         var tr = $(this).closest('tr');
         var row = runningTable.row( tr );
         var idx = $.inArray( tr.attr('id'), detailRows );

         if ( row.child.isShown() ) {
             tr.removeClass( 'details' );
             row.child.hide();

             // Remove from the 'open' array
             detailRows.splice( idx, 1 );
         }
         else {
             var rci = row.data();
             tr.addClass( 'details' );
             // put all the information into html for a single row
             var htmlRow = "<table class='table table-bordered table-striped table-condensed'>"
             htmlRow += "<thead><tr><th>#</th><th>Input Files</th><th>Size</th><th>Entries</th></tr></thead>";
             $.each( rci.inputFiles, function( key, value ) {
               htmlRow += "<tr><td>" + key + "</td>";
               htmlRow += "<td>" + value.metadataFileEntry + "</td>";
               htmlRow += "<td>" + bigNumberForSize(value.size) + "</td>";
               htmlRow += "<td>" + bigNumberForQuantity(value.entries) + "</td></tr>";
             });
             htmlRow += "</table>";
             htmlRow += "Output File: " + rci.outputFile + "<br>";
             htmlRow += rci.ecid;
             row.child(htmlRow).show();

             // Add to the 'open' array
             if ( idx === -1 ) {
                 detailRows.push( tr.attr('id') );
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
  * Generates the compactions table
  */
 function refreshECTables() {
   getCompactionCoordinator();
   var ecInfo = JSON.parse(sessionStorage.ecInfo);
   var ccAddress = ecInfo.server;
   var numCompactors = ecInfo.numCompactors;
   var lastContactTime = timeDuration(ecInfo.lastContact);
   console.log("compaction coordinator = " + ccAddress);
   console.log("numCompactors = " + numCompactors);
   $('#ccHostname').text(ccAddress);
   $('#ccNumQueues').text(ecInfo.numQueues);
   $('#ccNumCompactors').text(numCompactors);
   $('#ccLastContact').html(lastContactTime);

   // user paging is not reset on reload
   if(compactorsTable) compactorsTable.ajax.reload(null, false );
   if(runningTable) runningTable.ajax.reload(null, false );
 }

 /**
  * Get address of the compaction coordinator info
  */
 function getCompactionCoordinator() {
   $.getJSON('/rest/ec', function(data) {
        sessionStorage.ecInfo = JSON.stringify(data);
   });
 }

 function refreshCompactors() {
   console.log("Refresh compactors table.");
   // user paging is not reset on reload
   if(compactorsTable) compactorsTable.ajax.reload(null, false );
 }

 function refreshRunning() {
   console.log("Refresh running compactions table.");
   // user paging is not reset on reload
   if(runningTable) runningTable.ajax.reload(null, false );
 }
