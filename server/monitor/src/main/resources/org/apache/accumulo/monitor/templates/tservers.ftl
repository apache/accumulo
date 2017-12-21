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
      <div><h3>${title}</h3></div>
      <div class="center-block">
        <table id="badtservers" class="table table-bordered table-striped table-condensed">
            <caption><span class="table-caption">Non-Functioning Tablet Servers</span><br/>
                <span class="table-subcaption">The following tablet servers reported a status other than Online</span><br/></caption>
            <thead><tr>
                <th>Server</th>
                <th>Status</th></tr>
            </thead>
            <tbody></tbody>
        </table>
        <table id="deadtservers" class="table table-bordered table-striped table-condensed">
            <caption><span class="table-caption">Dead Tablet Servers</span><br/>
                <span class="table-subcaption">The following tablet servers are no longer reachable.</span><br/></caption>
            <thead><tr>
                <th>Server</th>
                <th class="duration">Last Updated</th>
                <th>Event</th>
                <th>Clear</th></tr>
            </thead>
            <tbody></tbody>
        </table>
        <table id="tservers" class="table table-bordered table-striped table-condensed">
            <thead><tr><th class="firstcell">Server&nbsp;</th>
                <th class="big-num">Hosted&nbsp;Tablets&nbsp;</th>
                <th class="duration">Last&nbsp;Contact&nbsp;</th>
                <th title="The time it took for the tserver to return its status." class="duration">Response&nbsp;Time&nbsp;</th>
                <th title="Key/value pairs over each instance, table or tablet." class="big-num">Entries&nbsp;</th>
                <th title="The number of Key/Value pairs inserted. (Note that deletes are inserted)" class="big-num">Ingest&nbsp;</th>
                <th title="The number of key/value pairs returned to clients. (Not the number of scans)" class="big-num">Query&nbsp;</th>
                <th title="The amount of time that ingest operations are suspended while waiting for data to be written to disk." class="duration">Hold&nbsp;Time&nbsp;</th>
                <th title="Information about the scans threads. Shows how many threads are running and how much work is queued for the threads.">Running<br/>Scans&nbsp;</th>
                <th title="The action of flushing memory to disk. Multiple tablets can be compacted simultaneously, but sometimes they must wait for resources to be available. The number of tablets waiting for compaction are in parentheses.">Minor<br/>Compactions&nbsp;</th>
                <th title="The action of gathering up many small files and rewriting them as one larger file. The number of tablets waiting for compaction are in parentheses.">Major<br/>Compactions&nbsp;</th>
                <th title="The recent index cache hit rate." class="percent">Index Cache<br/>Hit Rate&nbsp;</th>
                <th title="The recent data cache hit rate." class="percent">Data Cache<br/>Hit Rate&nbsp;</th>
                <th title="The Unix one minute load average. The average number of processes in the run queue over a one minute interval."  class="big-num">OS&nbsp;Load&nbsp;</th>
            </tr>
            </thead>
            <tbody></tbody>
        </table>
      </div>
