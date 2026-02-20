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
    <div id="sserversStatusBanner" style="display: none;">
      <div id="sservers-banner-message" class="alert" role="alert"></div>
    </div>
    <div class="row">
      <div class="col-xs-12">
        <table id="sservers" class="table caption-top table-bordered table-striped table-condensed">
          <caption><span class="table-caption">Scan Servers</span><br />
            <span class="table-subcaption">The following scan servers reported status and scan metrics when available.</span><br />
          </caption>
          <thead>
            <tr>
              <th class="firstcell">Server</th>
              <th>Resource Group</th>
              <th class="duration">Last Contact</th>
              <th title="Number of files open for scans." class="big-num">Open Files</th>
              <th title="Number of queries made during scans." class="big-num">Queries</th>
              <th title="Count of scanned entries." class="big-num">Scanned Entries</th>
              <th title="Query count." class="big-num">Query Results</th>
              <th title="Query byte count." class="big-size">Query Result Bytes</th>
              <th title="Count of scans where a busy timeout happened." class="big-num">Busy Timeouts</th>
              <th title="Count of scan reservation conflicts." class="big-num">Reservation Conflicts</th>
              <th title="Number of scan threads with no associated client session." class="big-num">Zombie Threads</th>
              <th title="Server idle flag (1=true, 0=false)." class="big-num">Idle</th>
              <th title="Low memory detected flag (1=true, 0=false)." class="big-num">Low Mem</th>
              <th title="Count of scans paused due to low memory." class="big-num">Paused Mem</th>
              <th title="Count of scans returned early due to low memory." class="big-num">Early Mem</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
    </div>
