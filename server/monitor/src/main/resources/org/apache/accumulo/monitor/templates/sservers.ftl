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
        <h3>${title}</h3>
      </div>
    </div>
    <div class="row">
      <div class="col-xs-12">
        <table id="sservers" class="table caption-top table-bordered table-striped table-condensed">
          <caption><span class="table-caption">Online Scan Servers</span><br />
            <span class="table-subcaption">The following scan servers reported a status of Online.</span><br />
          </caption>
          <thead>
            <tr>
              <th class="firstcell">Server&nbsp;</th>
              <th>Resource&nbsp;Group&nbsp;</th>
              <th class="duration">Last&nbsp;Contact&nbsp;</th>
              <th title="Number of files open for scans." class="big-num">Open&nbsp;Files&nbsp;</th>
              <th title="Number of queries made during scans." class="big-num">Queries&nbsp;</th>
              <th title="Count of scanned entries." class="big-num">Scanned&nbsp;Entries&nbsp;</th>
              <th title="Query count." class="big-num">Query&nbsp;Results&nbsp;</th>
              <th title="Query byte count." class="big-size">Query&nbsp;Result&nbsp;Bytes&nbsp;</th>
              <th title="Count of scans where a busy timeout happened." class="big-num">Busy&nbsp;Timeouts&nbsp;</th>
              <th title="Count of scan reservation conflicts." class="big-num">Reservation&nbsp;Conflicts&nbsp;</th>
              <th title="Number of scan threads with no associated client session." class="big-num">Zombie&nbsp;Threads&nbsp;</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </div>
