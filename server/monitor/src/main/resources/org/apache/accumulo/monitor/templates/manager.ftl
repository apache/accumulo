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
    <div class="row">
       <div class="col-xs-12">
          <h3>Manager Server Overview</h3>
       </div>
    </div>
    <div id="managerBanner" style="display: none;">
        <div class="alert alert-danger" role="alert">Manager Server Not Running</div>
    </div>
    <table id="managerStatus" class="table caption-top table-bordered table-striped table-condensed">
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
    <br />
    <table id="recoveryList" class="table caption-top table-bordered table-striped table-condensed">
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
    <#include "${tablesTemplate}">