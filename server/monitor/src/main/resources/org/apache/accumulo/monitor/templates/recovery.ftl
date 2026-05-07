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
      <div>
        <table id="recovery-overview" class="table caption-top table-bordered table-striped table-condensed">
          <caption><span class="table-caption">Table Recovery Overview</span><br />
            <span>The Manager is reporting the following tablets need recovery:</span><br />
          </caption>
          <thead>
            <tr>
              <th>Root Table</th>
              <th>Metadata Table Tablets</th>
              <th>User Table Tablets</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
      <div>
        <table id="tablets-needing-recovery" class="table caption-top table-bordered table-striped table-condensed">
          <caption><span class="table-caption">Tablets Requiring Recovery</span><br />
            <span>The system tables are reporting the following tablets need recovery. The Root and Metadata tables may not appear in this list.</span><br />
          </caption>
          <thead>
            <tr>
              <th>Table Id</th>
              <th>TabletId</th>
              <th>Tablet Directory</th>
              <th>Location</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
      <div>
        <table id="servers-sorting" class="table caption-top table-bordered table-striped table-condensed">
          <caption><span class="table-caption">Servers Sorting WALs</span><br />
            <span>The following servers have reported WAL sort activity.<br />
            Compactors will sort WAL files when not performing a compaction.<br />
            Scan Servers and Tablet Servers will sort WAL files
            in accordance with their properties ("sserver.wal.sort.concurrent.max` and `tserver.wal.sort.concurrent.max`)</span><br />
          </caption>          
          <thead>
            <tr>
              <th>Server</th>
              <th>Resource Group</th>
              <th>Server Type</th>
              <th>WAL Sorts In Progress</th>
              <th>WAL Sorts Avg Progress</th>
              <th>WAL Sorts Longest Duration</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
      <div>
        <table id="servers-replaying" class="table caption-top table-bordered table-striped table-condensed">
          <caption>
            <span class="table-caption">Tablet Servers Recovering Tablets</span><br />
            <span>The following Tablet Servers have reported Tablet recovery activity.</span><br />
          </caption>          
          <thead>
            <tr>
              <th>Server</th>
              <th>Resource Group</th>
              <th>Recoveries Started</th>
              <th>Recoveries Completed</th>
              <th>Recoveries Failed</th>
              <th>Recoveries In Progress</th>
              <th>Mutations Replayed</th>
            </tr>
          </thead>
          <tbody></tbody>
        </table>
      </div>
      