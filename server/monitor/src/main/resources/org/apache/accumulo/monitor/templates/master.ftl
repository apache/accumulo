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
      <div id="masterBanner" style="display: none;"><div class="alert alert-danger" role="alert">Master Server Not Running</div></div>
      <div class="center-block">
        <table id="masterStatus" class="table table-bordered table-striped table-condensed">
          <tbody>
            <tr><th class="firstcell" title="The hostname of the master server">Hostname</th>
                <th title="Number of tablet servers currently available">Online TServers&nbsp;</th>
                <th title="The total number of tablet servers configured">TotalTServers&nbsp;</th>
                <th title="The last time files were cleaned-up from HDFS.">Last&nbsp;GC</th>
                <th title="Tables are broken down into ranges of rows called tablets.">Tablets</th>
                <th>Unassigned Tablets&nbsp;</th>
                <th title="The total number of key/value pairs in Accumulo">Entries</th>
                <th title="The number of Key/Value pairs inserted. (Note that deletes are considered inserted)">Ingest</th>
                <th title="The total number of Key/Value pairs read on the server side.  Not all may be returned because of filtering.">Entries Read</th>
                <th title="The total number of Key/Value pairs returned as a result of scans.">Entries Returned</th>
                <th title="The maximum amount of time that ingest has been held across all servers due to a lack of memory to store the records">Hold&nbsp;Time</th>
                <th title="The Unix one minute load average. The average number of processes in the run queue over a one minute interval.">OS&nbsp;Load</th></tr>
          </tbody>
        </table>
        <table id="recoveryList" class="table table-bordered table-striped table-condensed">
          <caption><span class="table-caption">Log&nbsp;Recovery</span><br/>
            <span class="table-subcaption">Some tablets were unloaded in an unsafe manner. Write-ahead logs are being recovered.</span><br/>
          </caption>
            <thead><tr>
                <th>Server</th>
                <th>Log</th>
                <th>Time</th>
                <th>Progress</th></tr>
            </thead>
            <tbody></tbody>
        </table>
      </div>
      <br/>
      <#include "${tablesTemplate}">
