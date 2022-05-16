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
    <div id="managerBanner" style="display: none;">
        <div class="alert alert-danger" role="alert">Manager Server Not Running</div>
    </div>
    <table id="managerStatus" class="table table-bordered table-striped table-condensed">
        <caption><span class="table-caption">${title}</span><br /></caption>
        <thead>
            <tr>
                <th class="firstcell" title="The hostname of the manager server">Hostname</th>
                <th title="Number of tablet servers currently available">Online TServers&nbsp;</th>
                <th title="The total number of tablet servers configured">TotalTServers&nbsp;</th>
                <th title="The last time files were cleaned-up from HDFS.">Last&nbsp;GC</th>
                <th class="big-num" title="Tables are broken down into ranges of rows called tablets.">Tablets</th>
                <th class="big-num">Unassigned Tablets&nbsp;</th>
                <th class="big-num" title="The total number of key/value pairs in Accumulo">Entries</th>
                <th class="big-num-rounded" title="The number of Key/Value pairs inserted. (Note that deletes are considered inserted)">Ingest</th>
                <th class="big-num-rounded" title="The total number of Key/Value pairs read on the server side.  Not all may be returned because of filtering.">Entries Read</th>
                <th class="big-num-rounded" title="The total number of Key/Value pairs returned as a result of scans.">Entries Returned</th>
                <th class="duration" title="The maximum amount of time that ingest has been held across all servers due to a lack of memory to store the records">Hold&nbsp;Time</th>
                <th class="big-num" title="The Unix one minute load average. The average number of processes in the run queue over a one minute interval.">OS&nbsp;Load</th>
            </tr>
        </thead>
        <tbody></tbody>
    </table>
    <br /><br />
    <table id="recoveryList" class="table table-bordered table-striped table-condensed">
        <caption><span class="table-caption">Log&nbsp;Recovery</span><br />
            <span class="table-subcaption">Some tablets were unloaded in an unsafe manner. Write-ahead logs are being
                recovered.</span><br />
        </caption>
        <thead>
            <tr>
                <th>Server</th>
                <th>Log</th>
                <th class="duration">Time</th>
                <th class="percent">Progress</th>
            </tr>
        </thead>
        <tbody></tbody>
    </table>
    <br />
    <table id="tableList" class="table table-bordered table-striped table-condensed">
        <caption><span class="table-caption">Table&nbsp;Status</span><br /></caption>
        <thead>
            <tr>
                <th>Table&nbsp;Name</th>
                <th>State</th>
                <th title="Tables are broken down into ranges of rows called tablets." class="big-num">Tablets</th>
                <th title="Tablets unavailable for query or ingest. May be a transient condition when tablets are moved for balancing." class="big-num">Offline</th>
                <th title="Key/value pairs over each instance, table or tablet." class="big-num">Entries</th>
                <th title="The total number of key/value pairs stored in memory and not yet written to disk." class="big-num">In&nbsp;Mem</th>
                <th title="The rate of Key/Value pairs inserted. (Note that deletes are considered inserted)" class="big-num">Ingest</th>
                <th title="The rate of Key/Value pairs read on the server side. Not all key values read may be returned to client because of filtering." class="big-num">Read</th>
                <th title="The rate of Key/Value pairs returned to clients during queries. This is not the number of scans." class="big-num">Returned</th>
                <th title="The amount of time live ingest operations (mutations, batch writes) have been waiting for the tserver to free up memory." class="duration">Hold&nbsp;Time</th>
                <th title="Running scans. The number queued waiting are in parentheses.">Scans</th>
                <th title="Minor Compactions. The number of tablets waiting for compaction are in parentheses.">MinC</th>
                <th title="Major Compactions. The number of tablets waiting for compaction are in parentheses.">MajC</th>
            </tr>
        </thead>
        <tbody></tbody>
    </table>