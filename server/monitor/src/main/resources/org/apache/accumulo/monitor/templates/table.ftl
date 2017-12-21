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
      <script>
        /**
         * Creates participating Tservers initial table, passes the table and tableID from the template
         */
        $(document).ready(function() {
          tableID = '${tableID}';
          refreshTable();

          // Create tooltip for table column information
          $(document).tooltip();
        });
      </script>
      <div><h3>${title}</h3></div>
      <div class="center-block">
      <table id="participatingTServers" class="table table-bordered table-striped table-condensed">
          <caption><span class="table-caption">${table}</span></caption>
          <tbody>
          <tr><th class="firstcell">Server&nbsp;</th>
          <th>Hosted&nbsp;Tablets&nbsp;</th>
          <th>Last&nbsp;Contact&nbsp;</th>
          <th title="Key/value pairs over each instance, table or tablet.">Entries&nbsp;</th>
          <th title="The number of Key/Value pairs inserted. (Note that deletes are considered inserted)">Ingest&nbsp;</th>
          <th title="The number of key/value pairs returned to clients. (Not the number of scans)">Query&nbsp;</th>
          <th title="The amount of time that ingest operations are suspended while waiting for data to be written to disk.">Hold&nbsp;Time&nbsp;</th>
          <th title="Information about the scans threads. Shows how many threads are running and how much work is queued for the threads.">Running<br/>Scans&nbsp;</th>
          <th title="The action of flushing memory to disk. Multiple tablets can be compacted simultaneously, but sometimes they must wait for resources to be available. The number of tablets waiting for compaction are in parentheses.">Minor<br/>Compactions&nbsp;</th>
          <th title="The action of gathering up many small files and rewriting them as one larger file.">Major<br/>Compactions&nbsp;</th>
          <th title="The recent index cache hit rate.">Index Cache<br/>Hit Rate&nbsp;</th>
          <th title="The recent data cache hit rate.">Data Cache<br/>Hit Rate&nbsp;</th>
          <th title="The Unix one minute load average. The average number of processes in the run queue over a one minute interval.">OS&nbsp;Load&nbsp;</th></tr>
          </tbody>
      </table>
      </div>
