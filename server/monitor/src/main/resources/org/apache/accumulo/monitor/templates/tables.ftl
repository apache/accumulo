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
        /**
         * Creates tables initial table
         */
        $(document).ready(function() {
          refreshTables();
          <#if tablesJs??>
            toggleMaster(true);
          </#if>

          // Create tooltip for table column information
          $(document).tooltip();
        });
      </script>
      <div><h3>${tablesTitle}</h3></div>
      <div id="tablesBanner"></div>
      <div class="center-block">
        <div>
          <select id="namespaces" data-placeholder="Select a namespace" multiple="multiple" style="width: 100%;">
          </select>
        </div>
      </div>
      <div class="center-block">
        <table id="tableList" class="table table-bordered table-striped table-condensed">
          <tbody>
            <tr><th class="firstcell sortable" onclick="sortTable(0)">Table&nbsp;Name&nbsp;<span class="glyphicon glyphicon-chevron-up"></span></th>
                <th onclick="sortTable(1)" class="sortable">State&nbsp;</th>
                <th onclick="sortTable(2)" title="Tables are broken down into ranges of rows called tablets." class="sortable">#&nbsp;Tablets&nbsp;</th>
                <th onclick="sortTable(3)" title="Tablets unavailable for query or ingest. May be a transient condition when tablets are moved for balancing." class="sortable">#&nbsp;Offline<br>Tablets&nbsp;</th>
                <th onclick="sortTable(4)" title="Key/value pairs over each instance, table or tablet." class="sortable">Entries&nbsp;</th>
                <th onclick="sortTable(5)" title="The total number of key/value pairs stored in memory and not yet written to disk." class="sortable">Entries<br>In&nbsp;Memory&nbsp;</th>
                <th onclick="sortTable(6)" title="The number of Key/Value pairs inserted. (Note that deletes are considered inserted)" class="sortable">Ingest&nbsp;</th>
                <th onclick="sortTable(7)" title="The number of Key/Value pairs read on the server side.Not all key values read may be returned to client because of filtering." class="sortable">Entries<br/>Read&nbsp;</th>
                <th onclick="sortTable(8)" title="The number of Key/Value pairs returned to clients during queries. This is not the number of scans." class="sortable">Entries<br/>Returned&nbsp;</th>
                <th onclick="sortTable(9)" title="The amount of time that ingest operations are suspended while waiting for data to be written to disk." class="sortable">Hold&nbsp;Time&nbsp;</th>
                <th onclick="sortTable(10)" title="Information about the scans threads. Shows how many threads are running and how much work is queued for the threads." class="sortable">Running<br/>Scans&nbsp;</th>
                <th onclick="sortTable(11)" title="The action of flushing memory to disk. Multiple tablets can be compacted simultaneously, but sometimes they must wait for resources to be available. The number of tablets waiting for compaction are in parentheses." class="sortable">Minor<br/>Compactions&nbsp;</th>
                <th onclick="sortTable(12)" title="The action of gathering up many small files and rewriting them as one larger file. The number of tablets waiting for compaction are in parentheses." class="sortable">Major<br/>Compactions&nbsp;</th></tr>
          </tbody>
        </table>
      </div>
