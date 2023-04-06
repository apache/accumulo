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
      <script>
        $(document).ready(function () {
          // initialize DataTables
          initServerTables('${server}');
        });
      </script>
      <div class="row">
        <div class="col-xs-12">
          <h3>${title}</h3>
        </div>
      </div>
      <div class="row">
        <div class="col-xs-12">
          <table id="tServerDetail" class="table caption-top table-bordered table-striped table-condensed">
            <caption><span class="table-caption">${server}</span></caption>
            <thead>
              <tr>
                <th class="big-num">Hosted&nbsp;Tablets&nbsp;</th>
                <th class="big-num">Entries&nbsp;</th>
                <th class="big-num">Minor&nbsp;Compacting&nbsp;</th>
                <th class="big-num">Major&nbsp;Compacting&nbsp;</th>
                <th class="big-num">Splitting&nbsp;</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
      </div>
      <br />
      <div class="row">
        <div class="col-xs-12">
          <table id="opHistoryDetails" class="table caption-top table-bordered table-striped table-condensed">
            <caption><span class="table-caption">All-Time&nbsp;Tablet&nbsp;Operation&nbsp;Results</span></caption>
            <thead>
              <tr>
                <th>Operation&nbsp;</th>
                <th class="big-num">Success&nbsp;</th>
                <th class="big-num">Failure&nbsp;</th>
                <th class="duration">Average<br />Queue&nbsp;Time&nbsp;</th>
                <th class="duration">Std.&nbsp;Dev.<br />Queue&nbsp;Time&nbsp;</th>
                <th class="duration">Average<br />Time&nbsp;</th>
                <th class="duration">Std.&nbsp;Dev.<br />Time&nbsp;</th>
                <th>Percentage&nbsp;Time&nbsp;Spent&nbsp;</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
      </div>
      <br />
      <div class="row">
        <div class="col-xs-12">
          <table id="currentTabletOps" class="table caption-top table-bordered table-striped table-condensed">
            <caption><span class="table-caption">Current&nbsp;Tablet&nbsp;Operation&nbsp;Results</span></caption>
            <thead>
              <tr>
                <th class="duration">Minor&nbsp;Average&nbsp;</th>
                <th class="duration">Minor&nbsp;Std&nbsp;Dev&nbsp;</th>
                <th class="duration">Major&nbsp;Avg&nbsp;</th>
                <th class="duration">Major&nbsp;Std&nbsp;Dev&nbsp;</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
      </div>
      <br />
      <div class="row">
        <div class="col-xs-12">
          <table id="perTabletResults" class="table caption-top table-bordered table-striped table-condensed">
            <caption><span class="table-caption">Detailed Tablet Operations</span></caption>
            <thead>
              <tr>
                <th>Table&nbsp;</th>
                <th title="Run 'getsplits -v' in the Accumulo Shell to associate the encoded tablets with their actual splits.">Tablet&nbsp;</th>
                <th class="big-num">Entries&nbsp;</th>
                <th class="big-num">Ingest&nbsp;</th>
                <th class="big-num">Query&nbsp;</th>
                <th class="duration">Minor&nbsp;Avg&nbsp;</th>
                <th class="duration">Minor&nbsp;Std&nbsp;Dev&nbsp;</th>
                <th class="big-num">Minor&nbsp;Avg&nbsp;e/s&nbsp;</th>
                <th class="duration">Major&nbsp;Avg&nbsp;</th>
                <th class="duration">Major&nbsp;Std&nbsp;Dev&nbsp;</th>
                <th class="big-num">Major&nbsp;Avg&nbsp;e/s&nbsp;</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
      </div>
