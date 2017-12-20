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
         * Creates server initial tables, passes values from template
         */
        $(document).ready(function() {
          serv = '${server}';
          refreshServer();
        });
      </script>
      <div><h3>${title}</h3></div>
      <div class="center-block">
        <table id="tServerDetail" class="table table-bordered table-striped table-condensed">
            <caption><span class="table-caption">Details</span><br/><span class="table-subcaption">'${server}'</span><br/></caption>
            <tbody>
            <tr><th class="firstcell" onclick="sortTable(0,0)">Hosted&nbsp;Tablets&nbsp;</th>
                <th onclick="sortTable(0,1)">Entries&nbsp;</th>
                <th onclick="sortTable(0,2)">Minor&nbsp;Compacting&nbsp;</th>
                <th onclick="sortTable(0,3)">Major&nbsp;Compacting&nbsp;</th>
                <th onclick="sortTable(0,4)">Splitting&nbsp;</th></tr>
            </tbody>
        </table>
      </div>
      <div class="center-block">
        <table id="opHistoryDetails" class="table table-bordered table-striped table-condensed">
            <caption><span class="table-caption">All-Time&nbsp;Tablet&nbsp;Operation&nbsp;Results</span><br/></caption>
            <tbody>
            <tr><th class="firstcell" onclick="sortTable(1,0)">Operation&nbsp;</th>
                <th onclick="sortTable(1,1)">Success&nbsp;</th>
                <th onclick="sortTable(1,2)">Failure&nbsp;</th>
                <th onclick="sortTable(1,3)">Average<br/>Queue&nbsp;Time&nbsp;</th>
                <th onclick="sortTable(1,4)">Std.&nbsp;Dev.<br/>Queue&nbsp;Time&nbsp;</th>
                <th onclick="sortTable(1,5)">Average<br/>Time&nbsp;</th>
                <th onclick="sortTable(1,6)">Std.&nbsp;Dev.<br/>Time&nbsp;</th>
                <th onclick="sortTable(1,7)">Percentage&nbsp;Time&nbsp;Spent&nbsp;</th></tr>
            </tbody>
        </table>
      </div>
      <div class="center-block">
        <table id="currentTabletOps" class="table table-bordered table-striped table-condensed">
            <caption><span class="table-caption">Current&nbsp;Tablet&nbsp;Operation&nbsp;Results</span><br/></caption>
            <tbody>
            <tr><th class="firstcell" onclick="sortTable(2,0)">Minor&nbsp;Average&nbsp;</th>
                <th onclick="sortTable(2,1)">Minor&nbsp;Std&nbsp;Dev&nbsp;</th>
                <th onclick="sortTable(2,2)">Major&nbsp;Avg&nbsp;</th>
                <th onclick="sortTable(2,3)">Major&nbsp;Std&nbsp;Dev&nbsp;</th></tr>
            </tbody>
        </table>
      </div>
      <div class="center-block">
        <table id="perTabletResults" class="table table-bordered table-striped table-condensed">
            <caption><span class="table-caption">Detailed&nbsp;Current&nbsp;Operations</span><br/><span class="table-subcaption">Per-tablet&nbsp;Details</span><br/></caption>
            <tbody>
            <tr><th class="firstcell" onclick="sortTable(3,0)">Table&nbsp;</th>
                <th onclick="sortTable(3,1)">Tablet&nbsp;</th>
                <th onclick="sortTable(3,2)">Entries&nbsp;</th>
                <th onclick="sortTable(3,3)">Ingest&nbsp;</th>
                <th onclick="sortTable(3,4)">Query&nbsp;</th>
                <th onclick="sortTable(3,5)">Minor&nbsp;Avg&nbsp;</th>
                <th onclick="sortTable(3,6)">Minor&nbsp;Std&nbsp;Dev&nbsp;</th>
                <th onclick="sortTable(3,7)">Minor&nbsp;Avg&nbsp;e/s&nbsp;</th>
                <th onclick="sortTable(3,8)">Major&nbsp;Avg&nbsp;</th>
                <th onclick="sortTable(3,9)">Major&nbsp;Std&nbsp;Dev&nbsp;</th>
                <th onclick="sortTable(3,10)">Major&nbsp;Avg&nbsp;e/s&nbsp;</th></tr>
            </tbody>
        </table>
      </div>
