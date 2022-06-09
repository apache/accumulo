<#--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
      <script>
        /**
         * Creates participating Tservers initial table, passes the tableID from the template
         */
        $(document).ready(function () {
            initTableServerTable('${tableID}');
          });
      </script>
      <div class="row">
        <div class="col-xs-12">
          <h3>${title}</h3>
        </div>
      </div>
      <div class="row">
        <div class="col-xs-12">
          <table id="participatingTServers" class="table table-bordered table-striped table-condensed">
            <caption><span class="table-caption">${table}</span></caption>
            <thead>
              <tr>
                <th rowspan="2">Server&nbsp;</th>
                <th rowspan="2" class="big-num">Hosted<br/>Tablets&nbsp;</th>
                <th rowspan="2" class="duration">Last&nbsp;Contact&nbsp;</th>
                <th rowspan="2" class="big-num" title="Key/value pairs over each instance, table or tablet.">Entries&nbsp;</th>
                <th rowspan="2" class="big-num" title="The number of Key/Value pairs inserted. (Note that deletes are considered inserted)">Ingest&nbsp;</th>
                <th rowspan="2" class="big-num" title="The number of key/value pairs returned to clients. (Not the number of scans)">Query&nbsp;</th>
                <th rowspan="2" class="duration" title="The amount of time live ingest operations (mutations, batch writes) have been waiting for the tserver to free up memory.">Hold&nbsp;Time&nbsp;</th>
                <th colspan="2" title="Information about the scans threads. Shows how many threads are running and how much work is queued for the threads.">Scans&nbsp;</th>
                <th colspan="2" title="The action of flushing memory to disk. Multiple tablets can be compacted simultaneously, but sometimes they must wait for resources to be available. The number of tablets waiting for compaction are in parentheses.">Minor&nbsp;Compactions&nbsp;</th>
                <th colspan="2" title="The action of gathering up many small files and rewriting them as one larger file.">Major&nbsp;Compactions&nbsp;</th>
                <th rowspan="2" class="percent" title="The recent index cache hit rate.">Index Cache<br/>Hit Rate&nbsp;</th>
                <th rowspan="2" class="percent" title="The recent data cache hit rate.">Data Cache<br/>Hit Rate&nbsp;</th>
                <th rowspan="2" class="big-num" title="The Unix one minute load average. The average number of processes in the run queue over a one minute interval.">OS&nbsp;Load&nbsp;</th>
              </tr>
              <tr>
                <th class="big-num">Running</th>
                <th class="big-num">Queued</th>
                <th class="big-num">Running</th>
                <th class="big-num">Queued</th>
                <th class="big-num">Running</th>
                <th class="big-num">Queued</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
      </div>
