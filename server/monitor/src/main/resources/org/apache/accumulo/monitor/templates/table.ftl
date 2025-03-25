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
        /**
         * Creates participating Tservers initial table, passes the tableId from the template
         */
        $(function () {
          initTableServerTable('${tableId}');
        });
      </script>
      <div class="row">
        <div class="col-xs-12">
          <h3>Details for table ${table} <small>(ID: ${tableId})</small></h3>
        </div>
      </div>
      <div class="row">
        <div class="col-xs-12">
          <table id="participatingTServers" class="table caption-top table-bordered table-striped table-condensed">
            <caption><span class="table-caption">Table Summary</span></caption>
            <thead>
              <tr>
                <th class="big-num">Entry Count</th>
                <th class="big-size">Size on disk</th>
                <th class="big-num">File Count</th>
                <th class="big-num">WAL Count</th>
                <th class="big-num">Total Tablet Count</th>
                <th class="big-num">Always Hosted Count</th>
                <th class="big-num">On Demand Count</th>
                <th class="big-num">Never Hosted Count</th>
                <th class="big-num">Assigned Count</th>
                <th class="big-num">Assigned To Dead Server Tablets</th>
                <th class="big-num">Hosted Tablets</th>
                <th class="big-num">Suspended Tablets</th>
                <th class="big-num">Unassigned Tablets</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
      </div>
      <br><br>
      <!-- Section for tablets details DataTable -->
      <div class="row">
        <div class="col-xs-12">
          <table id="tabletsList" class="table caption-top table-bordered table-striped table-condensed">
            <caption><span class="table-caption">Tablet Details</span></caption>
            <thead>
              <tr>
                <th>Tablet ID</th>
                <th class="big-size">Estimated Size</th>
                <th class="big-num">Estimated Entries</th>
                <th>Availability</th>
                <th class="big-num">Files</th>
                <th class="big-num">WALs</th>
                <th>Location</th>
              </tr>
            </thead>
            <tbody></tbody>
          </table>
        </div>
      </div>