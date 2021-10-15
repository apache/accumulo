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
 /**
  * Creates active compactions table
  */
 $(document).ready(function() {
    /*coordinatorTable = $('#coordinatorTable').DataTable({
           "ajax": {
             "url": '/rest/ec',
             "dataSrc": "ccInfo"
           },
           "stateSave": true,
           "dom": 'ti',
           "columns": [
             { "data": "server" },
             { "data": "numCompactors"}
           ]
         });*/

     // Create a table for compactors
     compactorsTable = $('#compactorsTable').DataTable({
       "ajax": {
         "url": '/rest/ec',
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
         { "data": "kind" },
         { "data": "queueName" },
         { "data": "ecid",
           "type": "html",
           "render": function ( data, type, row, meta ) {
             if(type === 'display') {
                if (row.ecid === 'none') {
                    data = "--";
                } else {
                  // only show last 5 chars of the ecid and include a link to pop up the whole thing
                  var len = row.ecid.length;
                  var shortened = row.ecid.substring(len - 5, len);
                  data = '<a href="javascript:alert(\'' + row.ecid + '\');">..' + shortened + '</a>';
                }
             }
             return data;
           }
         },
         { "data": "tableId" },
         { "data": "numFiles" },
         { "data": "outputFile"},
         { "data": "progress",
           "type": "html",
           "render": function ( data, type, row, meta ) {
              if(type === 'display') {
                  if (row.progress < 0) {
                    data = '--';
                  } else {
                    var p = Number(row.progress) * 100;
                    data = '<div class="progress"><div class="progress-bar" role="progressbar" style="min-width: 2em; width:' +
                         p + '%;">' + p + '%</div></div>';
                  }
              }
              return data;
            }
         }
       ]
     });
     refreshECTables();
 });

 /**
  * Used to redraw the page
  */
 function refresh() {
   refreshECTables();
 }
// $('#coordinatorTable > tbody:last-child').append('<tr><td></td></tr>');
 /**
  * Generates the compactions table
  */
 function refreshECTables() {
   getExternalCompactionInfo();
   if (sessionStorage.ccFound === 'true') {
     $('#ccBanner').hide();
     $('#ecDiv').show();

     var items = [];
     var ecInfo = JSON.parse(sessionStorage.ecInfo);
     var ccAddress = ecInfo.ccHost;
     var numCompactors = ecInfo.numCompactors;
     console.log("compaction coordinator = " + ccAddress);
     console.log("numCompactors = " + numCompactors);
     items.push(createFirstCell(ccAddress, ccAddress));
     items.push(createRightCell(numCompactors, numCompactors));
     $('<tr/>', {
          html: items.join('')
         }).appendTo('#coordinatorTable tbody');

     // user paging is not reset on reload
     if(compactorsTable) compactorsTable.ajax.reload(null, false );
   } else {
      $('#ccBanner').show();
      $('#ecDiv').hide();
   }
 }

 /**
  * Get address of the compaction coordinator info
  */
 function getExternalCompactionInfo() {
   $.getJSON('/rest/ec', function(data) {
        sessionStorage.ccFound = data.ccFound;
        sessionStorage.ecInfo = JSON.stringify(data);
   });
 }
