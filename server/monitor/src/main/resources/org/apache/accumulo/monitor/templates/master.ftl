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
        <table id="masterStatus" class="table table-bordered table-striped table-condensed" style="display: table;">
          <caption><span class="table-caption">Master&nbsp;Status</span><br/></caption>
          <tbody>
            <tr><th class="firstcell" onclick="sortMasterTable(0)" title="The hostname of the master server">Master&nbsp;</th>
                <th onclick="sortMasterTable(1)" title="Number of tablet servers currently available">#&nbsp;Online<br/>Tablet&nbsp;Servers&nbsp;</th>
                <th onclick="sortMasterTable(2)" title="The total number of tablet servers configured">#&nbsp;Total<br/>Tablet&nbsp;Servers&nbsp;</th>
                <th onclick="sortMasterTable(3)" title="The last time files were cleaned-up from HDFS.">Last&nbsp;GC&nbsp;</th>
                <th onclick="sortMasterTable(4)" title="Tables are broken down into ranges of rows called tablets.">#&nbsp;Tablets&nbsp;</th>
                <th onclick="sortMasterTable(5)" >#&nbsp;Unassigned<br>Tablets&nbsp;</th>
                <th onclick="sortMasterTable(6)" title="The total number of key/value pairs in Accumulo">Entries&nbsp;</th>
                <th onclick="sortMasterTable(7)" title="The number of Key/Value pairs inserted. (Note that deletes are considered inserted)">Ingest&nbsp;</th>
                <th onclick="sortMasterTable(8)" title="The total number of Key/Value pairs read on the server side.  Not all may be returned because of filtering.">Entries<br/>Read&nbsp;</th>
                <th onclick="sortMasterTable(9)" title="The total number of Key/Value pairs returned as a result of scans.">Entries<br/>Returned&nbsp;</th>
                <th onclick="sortMasterTable(10)" title="The maximum amount of time that ingest has been held across all servers due to a lack of memory to store the records">Hold&nbsp;Time&nbsp;</th>
                <th onclick="sortMasterTable(11)" title="The Unix one minute load average. The average number of processes in the run queue over a one minute interval.">OS&nbsp;Load&nbsp;</th></tr>
          </tbody>
        </table>
        <table id="recoveryList" class="table table-bordered table-striped table-condensed">
          <caption><span class="table-caption">Log&nbsp;Recovery</span><br/>
            <span class="table-subcaption">Some tablets were unloaded in an unsafe manner. Write-ahead logs are being recovered.</span><br/>
          </caption>
        </table>
      </div>
      <br/>
      <script src="/resources/js/${tablesJs}"></script>
      <#include "${tablesTemplate}">
